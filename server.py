"""
WalkieTalk — signaling + voice relay server
FastAPI + python-socketio (ASGI) + Redis pub/sub for multi-instance scale

Environment variables:
    SUPABASE_URL          = https://your-project.supabase.co
    SUPABASE_KEY          = your Supabase anon/service key
    REDIS_URL             = redis://localhost:6379   (empty = single-instance mode)
    AI_ASSISTANT_URL      = https://bot-voice-sqnz.onrender.com/ai-assistant
    AI_CHAT_URL           = optional dedicated text-chat endpoint; empty = use AI_ASSISTANT_URL
    AI_ASSISTANT_API_KEY  = same value as AI_API_KEY on the bot-voice server
                            (AI_API_KEY is also accepted as a fallback)
    MAX_SCREEN_SIGNAL_RATE= WebRTC screen-share signaling events per rate window
    MAX_SCREEN_SDP_CHARS  = max SDP offer/answer payload length
    MAX_SCREEN_ICE_CHARS  = max ICE candidate payload length
    RENDER_EXTERNAL_URL   = set automatically by Render — used for self-ping keepalive
                            (set SERVER_URL manually on other platforms)
    CORS_ORIGINS          = comma-separated allowed origins; default *

Screen sharing:
    The server does WebRTC signaling only. It relays offers, answers, ICE candidates,
    and room state. Video/audio screen tracks must be sent peer-to-peer by clients.

Run locally:
    uvicorn server:socket_app --host 0.0.0.0 --port 3000 --reload

Deploy (Render / Railway):
    Start: uvicorn server:socket_app --host 0.0.0.0 --port $PORT --workers 1
"""

import asyncio
import logging
import os
import re
import time
from collections import deque
from contextlib import asynccontextmanager

import datetime as _dt
import httpx
import socketio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("walkie")

# ── Config ────────────────────────────────────────────────────────────────────
def _env_int(name: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value

def _env_float(name: str, default: float, min_value: float | None = None, max_value: float | None = None) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value

SUPABASE_URL = os.environ.get("SUPABASE_URL",
    "https://bgqeqiyfgpdvgeepignt.supabase.co").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY",
    "sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6")
REDIS_URL    = os.environ.get("REDIS_URL", "")
AI_ASSISTANT_URL = os.environ.get("AI_ASSISTANT_URL",
    "https://bot-voice-sqnz.onrender.com/ai-assistant")
# Optional text chat endpoint. If empty, the server will try AI_ASSISTANT_URL
# with a text payload and return a clear setup message if the backend does not support it.
AI_CHAT_URL = os.environ.get("AI_CHAT_URL", "").strip()
# Send this key to bot-voice /ai-assistant. It must match AI_API_KEY on that server.
# Keep AI_CHAT_URL empty when /ai-assistant supports text JSON {"message": "..."}.
AI_ASSISTANT_API_KEY = (
    os.environ.get("AI_ASSISTANT_API_KEY")
    or os.environ.get("AI_API_KEY")
    or ""
).strip()
AI_TIMEOUT_SECS = _env_float("AI_TIMEOUT_SECS", 45.0, 5.0, 120.0)
AI_CHAT_TIMEOUT_SECS = _env_float("AI_CHAT_TIMEOUT_SECS", AI_TIMEOUT_SECS, 5.0, 120.0)

KEEPALIVE_URL = (
    os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    or os.environ.get("SERVER_URL", "").rstrip("/")
)

_CORS_ORIGINS_RAW = os.environ.get("CORS_ORIGINS", "*").strip() or "*"
if _CORS_ORIGINS_RAW == "*":
    CORS_ALLOWED_ORIGINS: str | list[str] = "*"
    FASTAPI_CORS_ORIGINS: list[str] = ["*"]
else:
    _origins = [o.strip() for o in _CORS_ORIGINS_RAW.split(",") if o.strip()]
    CORS_ALLOWED_ORIGINS = _origins or "*"
    FASTAPI_CORS_ORIGINS = _origins or ["*"]

_SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
}


def _ai_headers() -> dict[str, str]:
    """Headers for the external bot-voice AI assistant endpoint."""
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if AI_ASSISTANT_API_KEY:
        # bot-voice /ai-assistant accepts X-Api-Key. Authorization is added too
        # for deployments that use Bearer verification.
        headers["X-Api-Key"] = AI_ASSISTANT_API_KEY
        headers["Authorization"] = f"Bearer {AI_ASSISTANT_API_KEY}"
    return headers


# ── Constants ─────────────────────────────────────────────────────────────────
MAX_ROOM_SIZE   = _env_int("MAX_ROOM_SIZE", 20, 2, 200)
MAX_NAME_LEN    = 32
MAX_ROOM_LEN    = 40
MAX_AUDIO_BYTES = _env_int("MAX_AUDIO_BYTES", 8_000_000, 256_000, 24_000_000)
MAX_DURATION    = _env_float("MAX_DURATION", 65.0, 1.0, 300.0)
MAX_MSG_RATE    = _env_int("MAX_MSG_RATE", 4, 1, 60)
MSG_RATE_WINDOW = _env_float("MSG_RATE_WINDOW", 10.0, 1.0, 60.0)
MAX_CHUNK_BYTES = _env_int("MAX_CHUNK_BYTES", 220_000, 32_000, 1_500_000)
MAX_CHUNK_RATE  = _env_int("MAX_CHUNK_RATE", 40, 4, 120)  # live chunks per MSG_RATE_WINDOW
MAX_AI_TEXT_LEN = _env_int("MAX_AI_TEXT_LEN", 2_000, 64, 8_000)
MAX_AI_HISTORY  = _env_int("MAX_AI_HISTORY", 12, 0, 40)
MAX_AI_CHAT_RATE = _env_int("MAX_AI_CHAT_RATE", 8, 1, 60)

# Screen sharing is WebRTC signaling only. Do not relay video frames through Socket.IO.
MAX_SCREEN_SIGNAL_RATE = _env_int("MAX_SCREEN_SIGNAL_RATE", 50, 5, 240)
MAX_SCREEN_SDP_CHARS   = _env_int("MAX_SCREEN_SDP_CHARS", 80_000, 4_000, 250_000)
MAX_SCREEN_ICE_CHARS   = _env_int("MAX_SCREEN_ICE_CHARS", 16_000, 1_000, 80_000)
SCREEN_STATE_TTL       = _env_int("SCREEN_STATE_TTL", 6 * 3600, 60, 24 * 3600)

ZONE_TTL_SECS: int = _env_int("ZONE_TTL_SECS", 5 * 3600, 300, 7 * 24 * 3600)

# Redis key prefixes
_RK_ROOM     = "wt:room:"
_RK_USER     = "wt:user:"
_RK_RATE     = "wt:rate:"
_RK_PRESENCE = "wt:presence"
_RK_SCREEN   = "wt:screen:"
_USER_TTL    = 3600
_RATE_TTL    = int(MSG_RATE_WINDOW * 2)

# Pre-computed constant strings — avoids repeated str() on the hot path
_S_MAX_MSG_RATE  = str(MAX_MSG_RATE)
_S_MAX_CHUNK_RATE = str(MAX_CHUNK_RATE)
_S_MAX_AI_CHAT_RATE = str(MAX_AI_CHAT_RATE)
_S_MAX_SCREEN_SIGNAL_RATE = str(MAX_SCREEN_SIGNAL_RATE)
_S_RATE_TTL      = str(_RATE_TTL)
_S_MAX_ROOM      = str(MAX_ROOM_SIZE)
_S_USER_TTL      = str(_USER_TTL)

ALLOWED_MIME: frozenset[str] = frozenset({
    "audio/webm", "audio/webm;codecs=opus",
    "audio/mp4",  "audio/ogg", "audio/wav",
})

# ── Precompiled regexes ────────────────────────────────────────────────────────
_NAME_RE  = re.compile(r"[^a-z0-9_\-]")
_ROOM_RE  = re.compile(r"[^A-Z0-9_\-]")
_DEV_RE   = re.compile(r"[^a-zA-Z0-9_\-]")
_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{6}$")
_SDP_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_SDP_SSRC_MSID_RE = re.compile(r"^(a=ssrc:\d+)\s+msid:\s*([^\s]+)\s+([^\s]+).*$")
_SDP_LINE_TYPES = frozenset("vosiuepcbtrzkam")

_start_time = time.time()

# ── Lua scripts ───────────────────────────────────────────────────────────────

_LUA_RATE = """
local key    = KEYS[1]
local cutoff = ARGV[1]
local member = ARGV[2]
local score  = ARGV[3]
local limit  = tonumber(ARGV[4])
local ttl    = tonumber(ARGV[5])
redis.call('zremrangebyscore', key, '-inf', cutoff)
local count = redis.call('zcard', key)
if count >= limit then return 0 end
redis.call('zadd', key, score, member)
redis.call('expire', key, ttl)
return 1
"""

_LUA_JOIN = """
local room_key = KEYS[1]
local user_key = KEYS[2]
local pres_key = KEYS[3]
local sid      = ARGV[1]
local room     = ARGV[2]
local name     = ARGV[3]
local max_size = tonumber(ARGV[4])
local now      = ARGV[5]
local inst     = ARGV[6]
local ttl      = tonumber(ARGV[7])
local cur = redis.call('scard', room_key)
if cur >= max_size then return 0 end
redis.call('sadd', room_key, sid)
redis.call('hset', user_key, 'room', room, 'name', name, 'joined_at', now)
redis.call('expire', user_key, ttl)
redis.call('hset', pres_key, sid, inst)
redis.call('expire', room_key, ttl)
return 1
"""

_LUA_LEAVE = """
local user_key = KEYS[1]
local pres_key = KEYS[2]
local sid      = ARGV[1]
local room_pfx = ARGV[2]
local room = redis.call('hget', user_key, 'room')
local name = redis.call('hget', user_key, 'name')
local joined = redis.call('hget', user_key, 'joined_at')
if not room then return {nil, nil, nil} end
redis.call('del', user_key)
redis.call('hdel', pres_key, sid)
if room ~= '' then
    local rk = room_pfx .. room
    redis.call('srem', rk, sid)
    if redis.call('scard', rk) == 0 then
        redis.call('del', rk)
    end
end
return {room, name or '', joined or ''}
"""

# ── Shared clients ─────────────────────────────────────────────────────────────
_http:  httpx.AsyncClient | None = None
_ai_http: httpx.AsyncClient | None = None
_redis                           = None
_last_redis_fallback_log: float = 0.0


def _log_redis_fallback(context: str, exc: Exception) -> None:
    """Log Redis failures without flooding logs during a transient outage."""
    global _last_redis_fallback_log
    now = time.time()
    if now - _last_redis_fallback_log >= 30.0:
        _last_redis_fallback_log = now
        log.warning(
            "Redis %s failed; using local fallback where safe: %s",
            context, exc,
        )


# ── Local in-memory state ──────────────────────────────────────────────────────
_local_users:     dict[str, dict]         = {}
_local_rooms:     dict[str, set]          = {}
_local_msg_times: dict[str, deque[float]] = {}      # PTT / fallback generic rate
_local_chunk_times: dict[str, deque[float]] = {}    # live audio chunk rate
_local_signal_times: dict[str, deque[float]] = {}   # WebRTC signaling rate
_local_ai_times: dict[str, deque[float]] = {}       # AI chat rate
_local_screens: dict[str, dict] = {}                # room -> active screen share state

INSTANCE_ID = f"inst_{os.getpid()}_{int(time.time()) % 10000}"

# ── Health ping cache ──────────────────────────────────────────────────────────
_last_ping_ok:   bool  = False
_last_ping_time: float = 0.0
_PING_CACHE_TTL: float = 10.0

# Self-ping interval
KEEPALIVE_INTERVAL: int = 10 * 60   # 10 minutes

# ── Connection quality ─────────────────────────────────────────────────────────
QUALITY_INTERVAL:   float = 30.0
QUALITY_PING_TMO:   float = 5.0
QUALITY_RTT_WINDOW: int   = 5
QUALITY_CYCLE_WIN:  int   = 5

_quality: dict[str, dict] = {}


def _quality_score(rtts: deque, cycles: deque) -> tuple[int, float, float, float]:
    """Returns (score 0-100, median_rtt_ms, drop_pct, jitter_ms)."""
    if not rtts:
        return 100, 0.0, 0.0, 0.0

    s = sorted(rtts)
    n = len(s)
    median_rtt = s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    if n >= 2:
        mean   = sum(s) / n
        jitter = (sum((x - mean) ** 2 for x in s) / (n - 1)) ** 0.5
    else:
        jitter = 0.0

    # Inline drop count — deque.count is O(n) but window is ≤5
    drop_count = sum(1 for c in cycles if not c)
    drop_pct   = (drop_count / len(cycles) * 100) if cycles else 0.0

    lat_score = (
        50.0 if median_rtt <= 100 else
        50.0 - (median_rtt - 100) / 300 * 25 if median_rtt <= 400 else
        max(0.0, 25.0 - (median_rtt - 400) / 200 * 25)
    )
    drop_score = max(0.0, 30.0 - drop_pct / 50 * 30)
    jit_score  = (
        20.0 if jitter <= 20 else
        20.0 - (jitter - 20) / 130 * 10 if jitter <= 150 else
        max(0.0, 10.0 - (jitter - 150) / 100 * 10)
    )

    score = round(lat_score + drop_score + jit_score)
    return max(0, min(100, score)), round(median_rtt, 1), round(drop_pct, 1), round(jitter, 1)


async def _quality_task(sid: str) -> None:
    state = _quality.get(sid)
    if state is None:
        return
    cycle = 0
    try:
        while True:
            await asyncio.sleep(QUALITY_INTERVAL)
            nonce   = f"{sid}_{cycle}_{os.urandom(2).hex()}"
            sent_at = time.monotonic()
            state["pending"][nonce] = sent_at
            await sio.emit("quality_ping", {"nonce": nonce}, to=sid)
            cycle += 1

            await asyncio.sleep(QUALITY_PING_TMO)

            if nonce in state["pending"]:
                del state["pending"][nonce]
                state["cycles"].append(False)
                log.debug("quality drop  sid=%s  nonce=%s", sid[:8], nonce)

            score, median_rtt, drop_pct, jitter = _quality_score(
                state["rtts"], state["cycles"]
            )
            await sio.emit("quality_update", {
                "score":      score,
                "latency_ms": median_rtt,
                "drop_pct":   drop_pct,
                "jitter_ms":  jitter,
            }, to=sid)
            log.info(
                "   quality sid=%-8s  score=%3d  rtt=%.0fms  drop=%.0f%%  jitter=%.0fms",
                sid[:8], score, median_rtt, drop_pct, jitter,
            )
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log.error("quality_task sid=%s unexpected error: %s", sid[:8], exc, exc_info=True)


# ── Zone expiry background task ───────────────────────────────────────────────
ZONE_EXPIRY_INTERVAL: int = 15 * 60

async def _zone_expiry_task() -> None:
    log.info("Zone expiry task started  interval=%ds", ZONE_EXPIRY_INTERVAL)
    first_run = True
    while True:
        await asyncio.sleep(5 if first_run else ZONE_EXPIRY_INTERVAL)
        first_run = False
        if _http is None:
            continue
        try:
            r = await _http.delete(
                "/rest/v1/geo_zones",
                params={"expires_at": "lt.now()"},
                headers={"Prefer": "return=representation"},
            )
            if r.is_success:
                try:
                    deleted = r.json() if r.text and r.text.strip() not in ("", "[]") else []
                except Exception:
                    deleted = []
                if deleted:
                    log.info("Zone expiry: deleted %d expired zones", len(deleted))
                    await asyncio.gather(*[
                        sio.emit("zone_deleted", {
                            "id":        z.get("id", ""),
                            "device_id": z.get("device_id", ""),
                            "expired":   True,
                        })
                        for z in deleted
                    ])
            else:
                log.warning("Zone expiry DELETE failed %s: %s",
                            r.status_code, r.text[:200])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Zone expiry task error: %s", exc, exc_info=True)


# ── Render keepalive ──────────────────────────────────────────────────────────
async def _keepalive_task() -> None:
    if not KEEPALIVE_URL:
        log.info("Keepalive disabled — RENDER_EXTERNAL_URL / SERVER_URL not set")
        return

    url = f"{KEEPALIVE_URL}/health"
    log.info("Keepalive started  url=%s  interval=%ds", url, KEEPALIVE_INTERVAL)

    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            try:
                r = await client.get(url)
                try:
                    uptime = r.json().get("uptime_s", "?") if r.is_success else "?"
                except Exception:
                    uptime = "?"
                log.info("Keepalive ping  status=%d  uptime=%ss", r.status_code, uptime)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Keepalive ping failed: %s", exc)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _http, _ai_http, _redis

    _http = httpx.AsyncClient(
        base_url=SUPABASE_URL,
        headers=_SB_HEADERS,
        timeout=10.0,
        limits=httpx.Limits(
            max_connections=20,
            max_keepalive_connections=5,
            keepalive_expiry=30,
        ),
    )
    _ai_http = httpx.AsyncClient(timeout=AI_TIMEOUT_SECS)

    if REDIS_URL:
        try:
            try:
                from redis import asyncio as _aioredis
            except ImportError:
                import aioredis as _aioredis          # type: ignore[no-redef]
            _redis = await _aioredis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            await _redis.ping()
            log.info("Redis connected  url=%s  instance=%s", REDIS_URL[:40], INSTANCE_ID)
        except Exception as exc:
            log.warning("Redis unavailable (%s) — single-instance mode", exc)
            _redis = None
    else:
        log.info("No REDIS_URL — single-instance mode  instance=%s", INSTANCE_ID)

    log.info("WalkieTalk started  pid=%d", os.getpid())

    _expiry_task = asyncio.create_task(_zone_expiry_task(), name="zone_expiry")
    _ka_task     = asyncio.create_task(_keepalive_task(),   name="keepalive")

    yield

    # Shutdown: background tasks first
    for _t in (_expiry_task, _ka_task):
        _t.cancel()
    await asyncio.gather(_expiry_task, _ka_task, return_exceptions=True)

    # Cancel all quality tasks before Redis closes
    for sid, q in list(_quality.items()):
        if (t := q.get("task")) and not t.done():
            t.cancel()
    if _quality:
        await asyncio.gather(*[q["task"] for q in _quality.values()
                                if q.get("task") and not q["task"].done()],
                             return_exceptions=True)
    _quality.clear()
    _local_screens.clear()

    if _redis:
        try:
            all_presence = await _redis.hgetall(_RK_PRESENCE)
            mine = [s for s, iid in all_presence.items() if iid == INSTANCE_ID]
            if mine:
                await _redis.hdel(_RK_PRESENCE, *mine)
                await asyncio.gather(
                    *[_redis_leave(sid, known_room=None) for sid in mine],
                    return_exceptions=True,
                )
            log.info("Shutdown cleanup: removed %d stale presences", len(mine))
        except Exception as exc:
            log.warning("Redis cleanup error: %s", exc)
        finally:
            await _redis.aclose()

    if _ai_http:
        await _ai_http.aclose()
    if _http:
        await _http.aclose()
    log.info("WalkieTalk stopped  instance=%s", INSTANCE_ID)


# ── Socket.IO ──────────────────────────────────────────────────────────────────
def _build_sio() -> socketio.AsyncServer:
    common = dict(
        async_mode="asgi",
        cors_allowed_origins=CORS_ALLOWED_ORIGINS,
        ping_timeout=60,
        ping_interval=25,
        max_http_buffer_size=MAX_AUDIO_BYTES + 512_000,
        logger=False,
        engineio_logger=False,
    )
    if REDIS_URL:
        try:
            mgr = socketio.AsyncRedisManager(REDIS_URL, channel="walkie_sio")
            log.info("AsyncRedisManager ready")
            return socketio.AsyncServer(client_manager=mgr, **common)
        except Exception as exc:
            log.warning("AsyncRedisManager failed (%s) — memory manager", exc)
    return socketio.AsyncServer(**common)


sio = _build_sio()

app = FastAPI(title="WalkieTalk", docs_url=None, redoc_url=None, lifespan=_lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=FASTAPI_CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)
socket_app = socketio.ASGIApp(sio, app)


# ── Redis helpers ─────────────────────────────────────────────────────────────

async def _redis_atomic_join(sid: str, room: str, name: str) -> bool:
    now_ts = time.time()
    if not _redis:
        if len(_local_rooms.get(room, set())) >= MAX_ROOM_SIZE:
            return False
        # joined_at stored as float — consistent with Redis path
        _local_users[sid] = {"room": room, "name": name, "joined_at": now_ts}
        _local_rooms.setdefault(room, set()).add(sid)
        return True

    try:
        result = await _redis.eval(
            _LUA_JOIN, 3,
            _RK_ROOM + room, _RK_USER + sid, _RK_PRESENCE,
            sid, room, name,
            _S_MAX_ROOM, f"{now_ts:.3f}", INSTANCE_ID, _S_USER_TTL,
        )
        admitted = bool(result)
    except Exception as exc:
        _log_redis_fallback("join", exc)
        if len(_local_rooms.get(room, set())) >= MAX_ROOM_SIZE:
            return False
        admitted = True

    if admitted:
        # Mirror in local state — float consistent with local path
        _local_users[sid] = {"room": room, "name": name, "joined_at": now_ts}
        _local_rooms.setdefault(room, set()).add(sid)
    return admitted


async def _redis_leave(sid: str, known_room: str | None) -> tuple[str | None, str, str | None]:
    if not _redis:
        return None, sid[:6], None

    try:
        result = await _redis.eval(
            _LUA_LEAVE, 2,
            _RK_USER + sid, _RK_PRESENCE,
            sid, _RK_ROOM,
        )
    except Exception as exc:
        _log_redis_fallback("leave", exc)
        return known_room, sid[:6], None
    if not result or result[0] is None:
        return known_room, sid[:6], None
    return result[0] or known_room, result[1] or sid[:6], result[2] or None


async def _redis_room_members(room: str) -> list[dict]:
    if not _redis:
        return _local_room_members(room)
    try:
        sids = await _redis.smembers(_RK_ROOM + room)
        if not sids:
            return []
        sids = list(sids)[:MAX_ROOM_SIZE]
        pipe = _redis.pipeline(transaction=False)
        for sid in sids:
            pipe.hget(_RK_USER + sid, "name")
        names = await pipe.execute()
        return [{"sid": s, "name": n} for s, n in zip(sids, names) if n]
    except Exception as exc:
        _log_redis_fallback("room_members", exc)
        return _local_room_members(room)


async def _redis_check_rate(sid: str, limit_str: str | None = None, key_suffix: str = "") -> bool:
    """
    Unified rate checker for PTT, live chunks, AI chat, and WebRTC signaling.
    key_suffix=""        -> PTT key
    key_suffix=":live"   -> live audio chunks
    key_suffix=":ai"     -> AI chat
    key_suffix=":signal" -> screen-share signaling
    """
    _limit = limit_str or _S_MAX_MSG_RATE
    if not _redis:
        return _local_check_rate(sid, int(_limit), key_suffix)

    now    = time.time()
    cutoff = now - MSG_RATE_WINDOW
    key    = _RK_RATE + sid + key_suffix
    # Append 4 random hex chars so concurrent coroutines never collide on the same member
    member = f"{now:.6f}:{os.urandom(2).hex()}"

    try:
        result = await _redis.eval(
            _LUA_RATE, 1,
            key,
            f"{cutoff:.6f}", member, f"{now:.6f}",
            _limit, _S_RATE_TTL,
        )
        return bool(result)
    except Exception as exc:
        _log_redis_fallback("rate_limit", exc)
        return _local_check_rate(sid, int(_limit), key_suffix)


# ── Unified leave ─────────────────────────────────────────────────────────────
async def _leave_room(sid: str) -> tuple[str | None, str]:
    info     = _local_users.get(sid)
    known    = info.get("room") if info else None
    name_loc = info.get("name", sid[:6]) if info else sid[:6]
    joined   = info.get("joined_at") if info else None

    if info and known:
        room_set = _local_rooms.get(known)
        if room_set is not None:
            room_set.discard(sid)
            if not room_set:
                del _local_rooms[known]
    _local_users.pop(sid, None)
    _local_msg_times.pop(sid, None)
    _local_chunk_times.pop(sid, None)   # clean live rate state too
    _local_signal_times.pop(sid, None)
    _local_ai_times.pop(sid, None)

    if _redis:
        r_room, r_name, r_joined = await _redis_leave(sid, known_room=known)
        final_room = r_room or known
        final_name = r_name or name_loc
        if r_joined:
            joined = r_joined
    else:
        final_room = known
        final_name = name_loc

    if joined and final_room:
        try:
            duration_s = round(time.time() - float(joined))
            log.info("   session @%-16s  room=%-20s  duration=%ds",
                     final_name, final_room, duration_s)
        except (TypeError, ValueError):
            pass

    return final_room, final_name


# ── Local helpers ─────────────────────────────────────────────────────────────

def _local_room_members(room: str) -> list[dict]:
    room_set = _local_rooms.get(room, ())
    return [{"sid": s, "name": _local_users[s]["name"]}
            for s in room_set if s in _local_users]


def _local_rate_store(key_suffix: str) -> dict[str, deque[float]]:
    if key_suffix == ":live":
        return _local_chunk_times
    if key_suffix == ":signal":
        return _local_signal_times
    if key_suffix == ":ai":
        return _local_ai_times
    return _local_msg_times


def _local_check_rate(sid: str, limit: int, key_suffix: str = "") -> bool:
    """Local fallback rate checker used when Redis is disabled/unavailable."""
    now    = time.time()
    cutoff = now - MSG_RATE_WINDOW
    store  = _local_rate_store(key_suffix)
    times  = store.get(sid)
    if times is None:
        times = deque()   # no maxlen — cutoff loop is the eviction mechanism
        store[sid] = times
    while times and times[0] <= cutoff:
        times.popleft()
    if len(times) >= limit:
        return False
    times.append(now)
    return True


# ── Room+name lookup — shared hot-path helper ─────────────────────────────────
async def _get_room_fast(sid: str) -> str | None:
    """Return room only — skips name lookup. Used by voice_chunk hot path."""
    info = _local_users.get(sid)
    if info:
        return info.get("room")
    if _redis:
        try:
            return await _redis.hget(_RK_USER + sid, "room")
        except Exception as exc:
            _log_redis_fallback("get_room", exc)
    return None


async def _get_room_and_name(sid: str) -> tuple[str | None, str]:
    """
    Fast local lookup with Redis fallback for cross-instance joins.
    Uses hmget (single round-trip) instead of two sequential hget calls.
    Returns (room_or_None, name).
    """
    info = _local_users.get(sid)
    if info:
        return info.get("room"), info.get("name", sid[:6])
    if _redis:
        # Single pipeline call — halves Redis latency vs two sequential hget
        try:
            vals = await _redis.hmget(_RK_USER + sid, "room", "name")
            return vals[0], (vals[1] or sid[:6])
        except Exception as exc:
            _log_redis_fallback("get_room_name", exc)
    return None, sid[:6]


# ── Sanitizers ────────────────────────────────────────────────────────────────
def _sanitize_name(raw: str, fallback: str) -> str:
    c = _NAME_RE.sub("", (raw or "").strip().lower().replace(" ", "_"))
    return c[:MAX_NAME_LEN] or fallback[:MAX_NAME_LEN]

def _sanitize_room(raw: str) -> str:
    return _ROOM_RE.sub("", (raw or "").strip().upper())[:MAX_ROOM_LEN]

def _sanitize_device(raw: str) -> str:
    return _DEV_RE.sub("", (raw or "").strip())[:128]

def _validate_color(raw: object) -> str:
    s = str(raw or "").strip()
    return s if _COLOR_RE.match(s) else "#007aff"


def _strip_data_url_base64(value: object) -> str:
    """Return plain base64 from either raw base64 or data:<mime>;base64,<payload>."""
    text = str(value or "").strip()
    if text.lower().startswith("data:") and "," in text:
        return text.split(",", 1)[1].strip()
    return text


# ── Screen sharing / WebRTC signaling helpers ───────────────────────────────

def _clean_small_text(value: object, limit: int = 128) -> str:
    text = str(value or "")
    text = re.sub(r"[\x00-\x1f\x7f]", "", text).strip()
    return text[:limit]


def _safe_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


def _sanitize_stream_id(value: object) -> str:
    stream_id = _DEV_RE.sub("", str(value or "").strip())[:48]
    return stream_id or f"screen_{int(time.time())}_{os.urandom(2).hex()}"


def _clean_screen_kind(value: object) -> str:
    kind = str(value or "screen").strip().lower()
    return kind if kind in {"screen", "window", "tab"} else "screen"


def _clean_webrtc_type(value: object, allowed: set[str], default: str) -> str:
    text = str(value or default).strip().lower()
    return text if text in allowed else default


def _clean_sdp(value: object) -> str | None:
    """Return a browser-safe SDP string or None.

    Some clients accidentally send nested RTCSessionDescription objects, literal
    escaped newlines, LF-only SDP, or malformed/empty lines. Browsers are strict
    in setRemoteDescription(), so normalize line endings and drop only obviously
    invalid SDP rows before relaying.
    """
    if isinstance(value, dict):
        value = value.get("sdp") or value.get("value") or ""
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw or len(raw) > MAX_SCREEN_SDP_CHARS:
        return None

    # Handle payloads that were accidentally double-escaped by a client.
    raw = raw.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    lines: list[str] = []
    dropped = 0
    for raw_line in raw.split("\n"):
        line = _SDP_CONTROL_RE.sub("", raw_line).strip()
        if not line:
            continue

        # Repair the common broken form: "a=ssrc:<id> msid: <stream> <track> ..."
        # and trim trailing garbage after the two msid tokens.
        if line.startswith("a=ssrc:") and " msid" in line:
            fixed = _SDP_SSRC_MSID_RE.match(line)
            if fixed:
                line = f"{fixed.group(1)} msid:{fixed.group(2)} {fixed.group(3)}"

        if len(line) < 2 or line[1] != "=" or line[0] not in _SDP_LINE_TYPES:
            dropped += 1
            continue
        lines.append(line)

    if not lines:
        return None

    # A valid SDP starts with v=0. If a noisy prefix was removed and v=0 exists
    # later, start from that point. Otherwise reject.
    if lines[0] != "v=0":
        try:
            v0 = lines.index("v=0")
            dropped += v0
            lines = lines[v0:]
        except ValueError:
            return None

    if dropped:
        log.debug("SDP sanitizer dropped %d malformed line(s)", dropped)

    return "\r\n".join(lines) + "\r\n"


def _sdp_from_data(data: dict, *keys: str) -> str | None:
    """Extract SDP from flat or nested signaling payloads."""
    for key in keys:
        sdp = _clean_sdp(data.get(key))
        if sdp:
            return sdp
    for key in ("description", "desc", "offer", "answer"):
        sdp = _clean_sdp(data.get(key))
        if sdp:
            return sdp
    return None


def _clean_ice_candidate(value: object) -> object | None:
    if isinstance(value, dict):
        candidate = str(value.get("candidate") or "").strip()
        if candidate.startswith("a=candidate:"):
            candidate = candidate[2:]
        if not candidate or len(candidate) > MAX_SCREEN_ICE_CHARS:
            return None

        idx = value.get("sdpMLineIndex")
        try:
            idx = int(idx) if idx is not None and str(idx).strip() != "" else None
        except (TypeError, ValueError):
            idx = None

        payload: dict[str, object] = {"candidate": candidate}
        sdp_mid = _clean_small_text(value.get("sdpMid"), 64)
        username_fragment = _clean_small_text(value.get("usernameFragment"), 128)
        if sdp_mid:
            payload["sdpMid"] = sdp_mid
        if idx is not None:
            payload["sdpMLineIndex"] = idx
        if username_fragment:
            payload["usernameFragment"] = username_fragment
        return payload

    if isinstance(value, str):
        value = value.strip()
        if value.startswith("a=candidate:"):
            value = value[2:]
        if not value or len(value) > MAX_SCREEN_ICE_CHARS:
            return None
        return value
    return None


def _public_screen_state(state: dict | None) -> dict | None:
    if not state:
        return None
    return {
        "room": state.get("room", ""),
        "stream_id": state.get("stream_id", ""),
        "sender_sid": state.get("sender_sid", ""),
        "sender_name": state.get("sender_name", ""),
        "kind": state.get("kind", "screen"),
        "title": state.get("title", ""),
        "has_audio": bool(state.get("has_audio", False)),
        "started_at": float(state.get("started_at") or 0.0),
    }


async def _get_screen_state(room: str) -> dict | None:
    if not room:
        return None
    state = _local_screens.get(room)
    if state:
        started_at = float(state.get("started_at") or 0.0)
        if started_at and (time.time() - started_at) <= SCREEN_STATE_TTL:
            return _public_screen_state(state)
        _local_screens.pop(room, None)
    if _redis:
        try:
            raw = await _redis.hgetall(_RK_SCREEN + room)
        except Exception as exc:
            _log_redis_fallback("get_screen_state", exc)
            raw = {}
        if raw:
            state = {
                "room": room,
                "stream_id": raw.get("stream_id", ""),
                "sender_sid": raw.get("sender_sid", ""),
                "sender_name": raw.get("sender_name", ""),
                "kind": raw.get("kind", "screen"),
                "title": raw.get("title", ""),
                "has_audio": raw.get("has_audio") == "1",
                "started_at": float(raw.get("started_at") or 0.0),
            }
            _local_screens[room] = state
            return _public_screen_state(state)
    return None


async def _set_screen_state(room: str, state: dict) -> None:
    _local_screens[room] = state
    if _redis:
        key = _RK_SCREEN + room
        try:
            await _redis.hset(key, mapping={
                "room": room,
                "stream_id": state["stream_id"],
                "sender_sid": state["sender_sid"],
                "sender_name": state["sender_name"],
                "kind": state.get("kind", "screen"),
                "title": state.get("title", ""),
                "has_audio": "1" if state.get("has_audio") else "0",
                "started_at": f"{float(state.get('started_at') or time.time()):.3f}",
            })
            await _redis.expire(key, SCREEN_STATE_TTL)
        except Exception as exc:
            _log_redis_fallback("set_screen_state", exc)


async def _clear_screen_state(room: str) -> dict | None:
    state = _local_screens.pop(room, None)
    if _redis:
        try:
            raw = await _redis.hgetall(_RK_SCREEN + room)
            await _redis.delete(_RK_SCREEN + room)
        except Exception as exc:
            _log_redis_fallback("clear_screen_state", exc)
            raw = {}
        if raw and not state:
            state = {
                "room": room,
                "stream_id": raw.get("stream_id", ""),
                "sender_sid": raw.get("sender_sid", ""),
                "sender_name": raw.get("sender_name", ""),
                "kind": raw.get("kind", "screen"),
                "title": raw.get("title", ""),
                "has_audio": raw.get("has_audio") == "1",
                "started_at": float(raw.get("started_at") or 0.0),
            }
    return _public_screen_state(state)


async def _sid_in_room(target_sid: str, room: str) -> bool:
    if not target_sid or not room:
        return False
    info = _local_users.get(target_sid)
    if info and info.get("room") == room:
        return True
    if _redis:
        try:
            target_room = await _redis.hget(_RK_USER + target_sid, "room")
            return target_room == room
        except Exception as exc:
            _log_redis_fallback("sid_in_room", exc)
    return False


async def _emit_screen_error(sid: str, code: str, msg: str, data: dict | None = None) -> None:
    payload = {"code": code, "msg": msg}
    if data:
        payload.update(data)
    await sio.emit("screen_share_error", payload, to=sid)


async def _stop_screen_share_for_sid(sid: str, reason: str = "stopped") -> None:
    """Stop any active screen share owned by sid. Used on leave/disconnect."""
    room = None
    for room_name, state in list(_local_screens.items()):
        if state.get("sender_sid") == sid:
            room = room_name
            break
    if room is None and _redis:
        # Best effort: scan only screen keys. This runs on leave/disconnect, not hot path.
        try:
            async for key in _redis.scan_iter(match=f"{_RK_SCREEN}*"):
                raw = await _redis.hgetall(key)
                if raw.get("sender_sid") == sid:
                    room = key[len(_RK_SCREEN):]
                    break
        except Exception as exc:
            log.warning("screen cleanup scan failed for sid=%s: %s", sid[:8], exc)
    if not room:
        return
    state = await _clear_screen_state(room)
    await sio.emit("screen_share_stopped", {
        "room": room,
        "stream_id": (state or {}).get("stream_id", ""),
        "sender_sid": sid,
        "reason": reason,
    }, room=room)
    log.info("   screen_stop sid=%-8s room=%-18s reason=%s", sid[:8], room, reason)


# ── AI chat helpers ───────────────────────────────────────────────────────────
def _clean_ai_text(value: object, limit: int = MAX_AI_TEXT_LEN) -> str:
    text = str(value or "")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text).strip()
    return text[:limit]


def _clean_ai_history(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    cleaned: list[dict[str, str]] = []
    for item in value[-MAX_AI_HISTORY:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").lower()
        if role not in {"user", "assistant"}:
            continue
        text = _clean_ai_text(item.get("text") or item.get("content"), 800)
        if text:
            cleaned.append({"role": role, "text": text})
    return cleaned


def _extract_ai_reply(payload: object) -> str:
    if isinstance(payload, str):
        return _clean_ai_text(payload, 6000)
    if not isinstance(payload, dict):
        return ""

    # Common simple response shapes.
    for key in ("text", "reply", "response", "answer", "message", "content"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return _clean_ai_text(value, 6000)

    # OpenAI/Anthropic-like response shapes, without requiring provider SDKs.
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            msg = first.get("message")
            if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                return _clean_ai_text(msg["content"], 6000)
            if isinstance(first.get("text"), str):
                return _clean_ai_text(first["text"], 6000)

    content = payload.get("content")
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                parts.append(part["text"])
            elif isinstance(part, str):
                parts.append(part)
        if parts:
            return _clean_ai_text("\n".join(parts), 6000)
    return ""


async def _call_ai_chat_backend(text: str, username: str, room: str, history: list[dict[str, str]]) -> str:
    """Call a configurable text-AI endpoint and normalize its response."""
    urls: list[str] = []
    if AI_CHAT_URL:
        urls.append(AI_CHAT_URL)
    elif AI_ASSISTANT_URL:
        # Backward-compatible fallback for deployments that extend /ai-assistant
        # to accept text as well as audio.
        urls.append(AI_ASSISTANT_URL)

    if not urls:
        return (
            "AI chat backend is not configured yet. Set AI_CHAT_URL to a POST endpoint "
            "that accepts JSON {text, message, username, room, history} and returns {text}."
        )

    payload = {
        "text": text,
        "message": text,
        "prompt": text,
        "username": username,
        "room": room,
        "history": history,
        "source": "walkietalk_ai_chat",
    }

    last_error = ""
    client = _ai_http or httpx.AsyncClient(timeout=AI_CHAT_TIMEOUT_SECS)
    close_client = _ai_http is None
    try:
        for url in urls:
            try:
                resp = await client.post(
                    url,
                    json=payload,
                    headers=_ai_headers(),
                    timeout=AI_CHAT_TIMEOUT_SECS,
                )
            except httpx.TimeoutException:
                raise
            except Exception as exc:
                last_error = str(exc)
                continue

            if not resp.is_success:
                last_error = f"AI backend HTTP {resp.status_code}: {resp.text[:180]}"
                continue

            try:
                data = resp.json()
            except ValueError:
                reply = _clean_ai_text(resp.text, 6000)
                if reply:
                    return reply
                last_error = "AI backend returned empty non-JSON response"
                continue

            reply = _extract_ai_reply(data)
            if reply:
                return reply
            last_error = "AI backend returned JSON without text/reply/response"
    finally:
        if close_client:
            await client.aclose()

    if AI_CHAT_URL:
        raise RuntimeError(last_error or "AI backend returned no usable response")
    setup_hint = (
        "AI assistant endpoint did not return a text chat reply. "
        "Set AI_ASSISTANT_API_KEY to the same value as AI_API_KEY on the bot-voice server, "
        "or set AI_CHAT_URL to a dedicated text chat endpoint."
    )
    return f"{setup_hint} Last error: {last_error[:180]}" if last_error else setup_hint


async def _build_ai_chat_reply(raw: object) -> dict[str, object]:
    if not isinstance(raw, dict):
        raw = {}
    text = _clean_ai_text(raw.get("text") or raw.get("message") or raw.get("prompt"))
    if not text:
        return {"ok": False, "error": "Message is empty"}
    if len(text) > MAX_AI_TEXT_LEN:
        return {"ok": False, "error": f"Message too long ({MAX_AI_TEXT_LEN} max)"}

    username = _clean_ai_text(raw.get("username"), MAX_NAME_LEN).lower() or "guest"
    username = _NAME_RE.sub("", username)[:MAX_NAME_LEN] or "guest"
    room = _sanitize_room(raw.get("room")) or "AI-CHAT"
    history = _clean_ai_history(raw.get("history"))

    reply = await _call_ai_chat_backend(text, username, room, history)
    return {"ok": True, "text": reply, "username": username, "room": room}

# ── HTTP endpoints ─────────────────────────────────────────────────────────────


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse({
        "name": "WalkieTalk",
        "status": "ok",
        "health": "/health",
        "zones": "/zones",
        "ai_chat": "/ai/chat",
        "socketio_path": "/socket.io",
        "features": {
            "voice_relay": True,
            "live_voice_chunks": True,
            "ai_chat": True,
            "geo_zones": True,
            "screen_share_signaling": True,
            "sdp_sanitizer": True,
        },
        "screen_share_events": [
            "screen_share_start", "screen_share_stop", "screen_share_state",
            "screen_viewer_ready", "screen_offer", "screen_answer", "screen_ice_candidate",
        ],
    })

@app.get("/health")
async def health() -> JSONResponse:
    global _last_ping_ok, _last_ping_time
    now = time.time()
    if _redis and (now - _last_ping_time) > _PING_CACHE_TTL:
        try:
            await _redis.ping()
            _last_ping_ok = True
        except Exception:
            _last_ping_ok = False
        _last_ping_time = now

    # Snapshot local state once — avoid repeated dict access
    conn  = len(_local_users)
    rooms = {k: len(v) for k, v in _local_rooms.items()}
    screens = {room: _public_screen_state(state) for room, state in _local_screens.items()}

    return JSONResponse({
        "status":      "ok",
        "instance":    INSTANCE_ID,
        "connections": conn,
        "rooms_local": rooms,
        "screen_shares_local": screens,
        "redis":       _last_ping_ok if _redis else None,
        "uptime_s":    round(now - _start_time),
    })



@app.post("/ai/chat")
async def ai_chat_http(request: Request) -> JSONResponse:
    """HTTP fallback for AI text chat when Socket.IO is not connected."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    # Rate limit by client IP for HTTP fallback.
    client_host = request.client.host if request.client else "unknown"
    rate_sid = "http_ai:" + _DEV_RE.sub("_", client_host)[:80]
    if not await _redis_check_rate(rate_sid, _S_MAX_AI_CHAT_RATE, ":ai"):
        return JSONResponse({"ok": False, "error": "AI chat rate limited"}, status_code=429)

    try:
        result = await _build_ai_chat_reply(body)
    except httpx.TimeoutException:
        return JSONResponse({"ok": False, "error": "AI timed out"}, status_code=504)
    except Exception as exc:
        log.exception("ai_chat_http error: %s", exc)
        return JSONResponse({"ok": False, "error": "AI chat backend offline"}, status_code=502)

    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return JSONResponse(result)


@app.get("/zones/ping")
async def zones_ping() -> JSONResponse:
    if _http is None:
        return JSONResponse({"ok": False, "error": "not ready"}, status_code=503)
    try:
        r = await _http.get("/rest/v1/geo_zones", params={"limit": "1", "select": "id"})
        return JSONResponse({"ok": r.is_success, "status": r.status_code, "body": r.text[:500]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/zones")
async def get_zones(request: Request) -> JSONResponse:
    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    try:
        r = await _http.get("/rest/v1/geo_zones", params={
            "order":      "created_at.asc",
            "select":     "id,device_id,name,channel,lat,lng,radius,color,auto_join,created_by,expires_at",
            "expires_at": "gte.now()",
        })
        if not r.is_success:
            log.error("Supabase GET failed %s: %s", r.status_code, r.text[:200])
            return JSONResponse({"error": "upstream error", "status": r.status_code}, status_code=502)
        return Response(content=r.content, media_type="application/json")
    except Exception as e:
        log.exception("get_zones: %s", e)
        return JSONResponse({"error": "server error"}, status_code=500)


@app.post("/zones")
async def upsert_zone(request: Request) -> JSONResponse:
    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    device_id  = _sanitize_device(body.get("device_id", ""))
    zone_id    = _sanitize_device(body.get("id", ""))
    name       = str(body.get("name", ""))[:40].strip()
    channel    = _sanitize_room(str(body.get("channel", "")))
    color      = _validate_color(body.get("color", "#007aff"))
    auto_join  = bool(body.get("auto_join", True))
    created_by = _sanitize_name(str(body.get("created_by", "")), "")

    try:
        lat    = float(body["lat"])
        lng    = float(body["lng"])
        radius = int(body["radius"])
    except (KeyError, TypeError, ValueError):
        return JSONResponse({"error": "lat/lng/radius required"}, status_code=400)

    if not device_id or not zone_id or not channel:
        return JSONResponse({"error": "device_id, id, channel required"}, status_code=400)
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        return JSONResponse({"error": "invalid coordinates"}, status_code=400)
    if not (10 <= radius <= 50_000):
        return JSONResponse({"error": "radius must be 10–50000 m"}, status_code=400)

    expires_ts = time.time() + ZONE_TTL_SECS
    expires_at = _dt.datetime.fromtimestamp(expires_ts, tz=_dt.timezone.utc).isoformat()

    payload = {
        "id": zone_id, "device_id": device_id,
        "name": name or channel, "channel": channel,
        "lat": lat, "lng": lng, "radius": radius,
        "color": color, "auto_join": auto_join,
        "created_by": created_by[:32],
        "expires_at": expires_at,
    }

    try:
        r = await _http.post(
            "/rest/v1/geo_zones", json=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
        if not r.is_success:
            log.error("Supabase upsert failed %s: %s", r.status_code, r.text[:200])
            return JSONResponse({"error": "upstream error", "status": r.status_code}, status_code=502)

        log.info("   zone_save  id=%-20s  ch=%-18s  dev=%s", zone_id[:20], channel, device_id[:12])
        await sio.emit("zone_upserted", {
            "id": zone_id, "device_id": device_id,
            "name": payload["name"], "channel": channel,
            "lat": lat, "lng": lng, "radius": radius,
            "color": color, "auto_join": auto_join,
            "created_by": created_by,
            "expires_at": expires_at,
        })
        return JSONResponse({"ok": True})
    except Exception as e:
        log.exception("upsert_zone: %s", e)
        return JSONResponse({"error": "server error"}, status_code=500)


@app.delete("/zones/{zone_id}")
async def delete_zone(zone_id: str, request: Request) -> JSONResponse:
    device_id = _sanitize_device(request.query_params.get("device_id", ""))
    zone_id   = _sanitize_device(zone_id)
    if not device_id or not zone_id:
        return JSONResponse({"error": "device_id and zone_id required"}, status_code=400)
    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    try:
        r = await _http.delete(
            "/rest/v1/geo_zones",
            params={"id": f"eq.{zone_id}", "device_id": f"eq.{device_id}"},
            headers={"Prefer": "return=representation"},
        )
        if not r.is_success:
            log.error("Supabase DELETE failed %s: %s", r.status_code, r.text[:200])
            return JSONResponse({"error": "upstream error", "status": r.status_code}, status_code=502)
        try:
            deleted_rows = r.json() if r.text and r.text.strip() not in ("", "[]") else []
        except Exception:
            deleted_rows = []
        if not deleted_rows:
            return JSONResponse({"error": "not found or not owner"}, status_code=404)
        await sio.emit("zone_deleted", {"id": zone_id, "device_id": device_id})
        return JSONResponse({"ok": True})
    except Exception as e:
        log.exception("delete_zone: %s", e)
        return JSONResponse({"error": "server error"}, status_code=500)


# ── Socket events ──────────────────────────────────────────────────────────────

@sio.event
async def connect(sid: str, environ: dict) -> None:
    origin = (environ.get("HTTP_ORIGIN") or environ.get("HTTP_REFERER") or "")[:60]
    log.info("[~] %-24s  origin=%s", sid, origin or "-")
    # Pre-create quality state BEFORE task starts — avoids race in _quality_task
    state: dict = {
        "pending": {},
        "rtts":    deque(maxlen=QUALITY_RTT_WINDOW),
        "cycles":  deque(maxlen=QUALITY_CYCLE_WIN),
        "task":    None,
    }
    _quality[sid] = state
    try:
        state["task"] = asyncio.create_task(_quality_task(sid), name=f"quality_{sid[:8]}")
    except RuntimeError as exc:
        # create_task can fail if the event loop is closing (e.g. shutdown race)
        log.warning("connect: could not create quality task for %s: %s", sid[:8], exc)


@sio.event
async def disconnect(sid: str) -> None:
    q = _quality.pop(sid, None)
    if q and (t := q.get("task")) and not t.done():
        t.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(t), timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass  # expected — task cancelled or slow to exit

    try:
        await _stop_screen_share_for_sid(sid, reason="disconnect")
        old_room, name = await _leave_room(sid)
    except Exception as exc:
        log.exception("_leave_room on disconnect sid=%s: %s", sid, exc)
        old_room, name = None, sid[:6]

    log.info("[-] %-24s @%-16s  room=%s", sid, name, old_room or "-")
    if old_room:
        await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)


@sio.event
async def join_room(sid: str, data: dict) -> None:
    try:
        if not isinstance(data, dict):
            log.warning("join_room: non-dict payload from %s: %r", sid[:8], type(data))
            return
        room = _sanitize_room(data.get("room", ""))
        name = _sanitize_name(data.get("name", ""), sid[:6])
        if not room:
            return

        await _stop_screen_share_for_sid(sid, reason="room_change")
        old_room, old_name = await _leave_room(sid)
        if old_room and old_room != room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": old_name}, room=old_room, skip_sid=sid)

        admitted = await _redis_atomic_join(sid, room, name)
        if not admitted:
            await sio.emit("error", {"code": "ROOM_FULL", "msg": f"Room full ({MAX_ROOM_SIZE} max)"}, to=sid)
            log.warning("Room %s full — rejected %s", room, sid)
            return

        await sio.enter_room(sid, room)

        members = await _redis_room_members(room) if _redis else _local_room_members(room)
        active_screen = await _get_screen_state(room)
        await asyncio.gather(
            sio.emit("peer_joined", {"sid": sid, "name": name}, room=room, skip_sid=sid),
            sio.emit("room_state",  {"members": members, "screen_share": active_screen}, to=sid),
        )
        log.info("[+] %-24s @%-16s  room=%-20s  n=%d", sid, name, room, len(members))

    except Exception as exc:
        log.exception("join_room sid=%s: %s", sid, exc)


@sio.event
async def leave_room_event(sid: str, data: dict) -> None:
    try:
        await _stop_screen_share_for_sid(sid, reason="leave_room")
        old_room, name = await _leave_room(sid)
        if old_room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)
    except Exception as exc:
        log.exception("leave_room_event sid=%s: %s", sid, exc)


@sio.event
async def update_name(sid: str, data: dict) -> None:
    try:
        if not isinstance(data, dict):
            return
        new_name = _sanitize_name(data.get("name", ""), "")
        if not new_name:
            return
        info = _local_users.get(sid)
        old_name = info["name"] if info else sid[:6]
        room     = info.get("room") if info else None

        name_changed = not info or info.get("name") != new_name
        if info:
            info["name"] = new_name
        if not room and _redis:
            room = await _redis.hget(_RK_USER + sid, "room")
        if _redis and name_changed:
            await _redis.hset(_RK_USER + sid, "name", new_name)
        if room:
            active_screen = await _get_screen_state(room)
            if active_screen and active_screen.get("sender_sid") == sid:
                local_state = _local_screens.get(room) or dict(active_screen)
                local_state["sender_name"] = new_name
                await _set_screen_state(room, local_state)
                await sio.emit("screen_share_state", {"screen_share": await _get_screen_state(room)}, room=room)
            await sio.emit("peer_name_updated", {"sid": sid, "name": new_name}, room=room, skip_sid=sid)
        log.info("   rename @%s -> @%s", old_name, new_name)
    except Exception as exc:
        log.exception("update_name sid=%s: %s", sid, exc)



@sio.event
async def ai_chat_message(sid: str, data: dict) -> None:
    """Text chat with AI. Sends response only to the requesting client."""
    msg_id = ""
    try:
        if not isinstance(data, dict):
            data = {}
        msg_id = str(data.get("msg_id") or "")[:80]

        if not await _redis_check_rate(sid, _S_MAX_AI_CHAT_RATE, ":ai"):
            await sio.emit("ai_chat_error", {"msg_id": msg_id, "error": "Slow down — too many AI messages"}, to=sid)
            return

        room, joined_name = await _get_room_and_name(sid)
        if not room:
            room = _sanitize_room(data.get("room")) or "AI-CHAT"
        if not joined_name or joined_name == sid[:6]:
            raw_name = str(data.get("username") or joined_name or "guest")
            joined_name = _NAME_RE.sub("", raw_name.lower())[:MAX_NAME_LEN] or "guest"

        body = dict(data)
        body["room"] = room
        body["username"] = joined_name

        await sio.emit("ai_chat_typing", {"msg_id": msg_id, "on": True}, to=sid)
        result = await _build_ai_chat_reply(body)
        await sio.emit("ai_chat_typing", {"msg_id": msg_id, "on": False}, to=sid)

        if not result.get("ok"):
            await sio.emit("ai_chat_error", {"msg_id": msg_id, "error": result.get("error") or "AI chat error"}, to=sid)
            return

        await sio.emit("ai_chat_response", {
            "msg_id": msg_id,
            "text": result.get("text") or "No response",
            "sender_name": "AI Assistant",
        }, to=sid)
        log.info("   ai_chat @%-14s room=%-18s %d chars", joined_name, room, len(str(data.get("text") or "")))
    except httpx.TimeoutException:
        await sio.emit("ai_chat_typing", {"msg_id": msg_id, "on": False}, to=sid)
        await sio.emit("ai_chat_error", {"msg_id": msg_id, "error": "AI timed out"}, to=sid)
    except Exception as exc:
        log.exception("ai_chat_message sid=%s: %s", sid, exc)
        await sio.emit("ai_chat_typing", {"msg_id": msg_id, "on": False}, to=sid)
        await sio.emit("ai_chat_error", {"msg_id": msg_id, "error": "AI chat backend offline"}, to=sid)


@sio.event
async def voice_message(sid: str, data: dict) -> None:
    try:
        if not isinstance(data, dict):
            return
        room, name = await _get_room_and_name(sid)
        if not room:
            return

        audio = data.get("audio") or ""
        if not audio or not isinstance(audio, str):
            return

        audio_len = len(audio)
        if audio_len > MAX_AUDIO_BYTES:
            await sio.emit("error", {"code": "MSG_TOO_LARGE", "msg": "Audio too large"}, to=sid)
            return

        if not await _redis_check_rate(sid):
            await sio.emit("error", {"code": "RATE_LIMITED", "msg": "Sending too fast"}, to=sid)
            log.warning("   rate_limited @%-16s  room=%s", name, room)
            return

        mime = str(data.get("mime") or "audio/webm")
        if mime not in ALLOWED_MIME:
            mime = "audio/webm"

        msg_id = str(data.get("msg_id") or "")[:64]
        try:
            duration = min(float(data.get("duration") or 0), MAX_DURATION)
        except (TypeError, ValueError):
            duration = 0.0

        await sio.emit(
            "voice_message",
            {"audio": audio, "mime": mime, "duration": round(duration, 1),
             "msg_id": msg_id, "sender_sid": sid, "sender_name": name},
            room=room, skip_sid=sid,
        )
        log.info("   voice @%-14s -> %-18s  %.1fs  %dB", name, room, duration, audio_len)

        # ── AI Chatbot Integration ──
        if room == "AI-BOT":
            await sio.emit("status_update", {"msg": "AI is thinking...", "cls": "warn"}, to=sid)
            if not AI_ASSISTANT_URL:
                await sio.emit("status_update", {"msg": "AI URL not configured", "cls": "err"}, to=sid)
                return
            try:
                client = _ai_http or httpx.AsyncClient(timeout=AI_TIMEOUT_SECS)
                close_client = _ai_http is None
                try:
                    resp = await client.post(
                        AI_ASSISTANT_URL,
                        json={
                            # bot-voice /ai-assistant JSON mode expects audio_base64/audio_mime.
                            "audio_base64": _strip_data_url_base64(audio),
                            "audio_mime": mime,
                            "message": "",
                            "username": name,
                            "room": room,
                            "source": "walkietalk_voice_message",
                        },
                        headers=_ai_headers(),
                        timeout=AI_TIMEOUT_SECS,
                    )
                finally:
                    if close_client:
                        await client.aclose()

                if not resp.is_success:
                    await sio.emit("status_update", {"msg": "AI server error", "cls": "err"}, to=sid)
                    log.error("AI Assistant API returned %s: %s", resp.status_code, resp.text[:200])
                    return

                try:
                    res_data = resp.json()
                except ValueError:
                    await sio.emit("status_update", {"msg": "AI returned invalid JSON", "cls": "err"}, to=sid)
                    return

                ai_audio = res_data.get("audio")
                ai_mime = str(res_data.get("mime") or "audio/webm")
                if ai_mime not in ALLOWED_MIME:
                    ai_mime = "audio/webm"
                try:
                    ai_duration = min(max(float(res_data.get("duration") or 5.0), 0.0), MAX_DURATION)
                except (TypeError, ValueError):
                    ai_duration = 5.0

                if isinstance(ai_audio, str) and ai_audio:
                    if len(ai_audio) > MAX_AUDIO_BYTES:
                        await sio.emit("status_update", {"msg": "AI audio too large", "cls": "err"}, to=sid)
                        log.warning("AI audio too large for @%s: %d chars", name, len(ai_audio))
                        return
                    await sio.emit(
                        "voice_message",
                        {
                            "audio": ai_audio,
                            "mime": ai_mime,
                            "duration": round(ai_duration, 1),
                            "msg_id": f"ai_{int(time.time())}_{os.urandom(2).hex()}",
                            "sender_sid": "ai_bot_sid",
                            "sender_name": "AI Assistant",
                        },
                        room=room,
                    )
                    await sio.emit("status_update", {"msg": "AI responded", "cls": "ok"}, to=sid)
                else:
                    ai_text = _extract_ai_reply(res_data) or str(res_data.get("error") or "No response")
                    ai_text = ai_text[:120]
                    await sio.emit("status_update", {"msg": f"AI text: {ai_text}", "cls": "ok"}, to=sid)
            except httpx.TimeoutException:
                await sio.emit("status_update", {"msg": "AI timed out", "cls": "err"}, to=sid)
            except Exception as e:
                await sio.emit("status_update", {"msg": "AI connection offline", "cls": "err"}, to=sid)
                log.exception("AI Assistant API connection error: %s", e)

    except Exception as exc:
        log.exception("voice_message sid=%s: %s", sid, exc)


@sio.event
async def voice_chunk(sid: str, data: dict) -> None:
    """Live voice streaming — relay a single audio chunk to the room immediately."""
    try:
        if not isinstance(data, dict):
            return
        # Fast path: only room is strictly needed for relay; name comes from local cache
        room = await _get_room_fast(sid)
        if not room:
            return
        name = (_local_users.get(sid) or {}).get("name", sid[:6])

        audio = data.get("audio") or ""
        if not audio or not isinstance(audio, str):
            return

        if len(audio) > MAX_CHUNK_BYTES:
            return  # silently drop oversized chunk

        # Rate limit: live chunks use separate key + higher limit
        if not await _redis_check_rate(sid, _S_MAX_CHUNK_RATE, ":live"):
            return  # silently drop — live stream self-regulates

        stream_id = str(data.get("stream_id") or "")[:32]
        try:
            seq = int(data.get("seq") or 0)
        except (TypeError, ValueError):
            seq = 0
        mime      = str(data.get("mime") or "audio/webm")
        if mime not in ALLOWED_MIME:
            mime = "audio/webm"

        await sio.emit(
            "voice_chunk",
            {"audio": audio, "mime": mime, "stream_id": stream_id,
             "seq": seq, "sender_sid": sid, "sender_name": name},
            room=room, skip_sid=sid,
        )

    except Exception as exc:
        log.exception("voice_chunk sid=%s: %s", sid, exc)


@sio.event
async def voice_stream_end(sid: str, data: dict) -> None:
    """Signal that a live stream ended — broadcast to room for cleanup."""
    try:
        if not isinstance(data, dict):
            data = {}
        room, name = await _get_room_and_name(sid)
        if not room:
            return

        stream_id = str(data.get("stream_id") or "")[:32]
        await sio.emit(
            "voice_stream_end",
            {"stream_id": stream_id, "sender_sid": sid, "sender_name": name},
            room=room, skip_sid=sid,
        )
        log.info("   live_end @%-14s -> %-18s  stream=%s", name, room, stream_id[:8])

    except Exception as exc:
        log.exception("voice_stream_end sid=%s: %s", sid, exc)


@sio.event
async def screen_share_start(sid: str, data: dict) -> None:
    """Start WebRTC screen sharing in the current room.

    Client flow:
      1) sharer emits screen_share_start
      2) viewers receive screen_share_started
      3) viewer emits screen_viewer_ready to sharer
      4) sharer sends screen_offer to viewer
      5) viewer sends screen_answer back
      6) both sides exchange screen_ice_candidate
    """
    try:
        if not isinstance(data, dict):
            data = {}
        if not await _redis_check_rate(sid, _S_MAX_SCREEN_SIGNAL_RATE, ":signal"):
            await _emit_screen_error(sid, "RATE_LIMITED", "Screen sharing signaling too fast")
            return

        room, name = await _get_room_and_name(sid)
        if not room:
            await _emit_screen_error(sid, "NOT_IN_ROOM", "Join a room before sharing your screen")
            return

        current = await _get_screen_state(room)
        allow_takeover = _safe_bool(data.get("takeover"), False)
        if current and current.get("sender_sid") != sid and not allow_takeover:
            await _emit_screen_error(sid, "SCREEN_BUSY", "Another user is already sharing", {"screen_share": current})
            return

        if current and current.get("sender_sid") != sid and allow_takeover:
            await _clear_screen_state(room)
            await sio.emit("screen_share_stopped", {
                "room": room,
                "stream_id": current.get("stream_id", ""),
                "sender_sid": current.get("sender_sid", ""),
                "reason": "takeover",
            }, room=room)

        state = {
            "room": room,
            "stream_id": _sanitize_stream_id(data.get("stream_id")),
            "sender_sid": sid,
            "sender_name": name,
            "kind": _clean_screen_kind(data.get("kind")),
            "title": _clean_small_text(data.get("title"), 120),
            "has_audio": _safe_bool(data.get("has_audio"), False),
            "started_at": time.time(),
        }
        await _set_screen_state(room, state)
        public_state = _public_screen_state(state)
        await sio.emit("screen_share_started", public_state, room=room)
        await sio.emit("screen_share_state", {"screen_share": public_state}, room=room)
        log.info("   screen_start @%-14s -> %-18s stream=%s", name, room, state["stream_id"][:12])
    except Exception as exc:
        log.exception("screen_share_start sid=%s: %s", sid, exc)
        await _emit_screen_error(sid, "SERVER_ERROR", "Could not start screen sharing")


@sio.event
async def screen_share_stop(sid: str, data: dict) -> None:
    try:
        if not isinstance(data, dict):
            data = {}
        room, _name = await _get_room_and_name(sid)
        if not room:
            return
        current = await _get_screen_state(room)
        if not current:
            await sio.emit("screen_share_state", {"screen_share": None}, to=sid)
            return
        if current.get("sender_sid") != sid:
            await _emit_screen_error(sid, "NOT_OWNER", "Only the active sharer can stop this screen share")
            return
        state = await _clear_screen_state(room)
        await sio.emit("screen_share_stopped", {
            "room": room,
            "stream_id": (state or current).get("stream_id", ""),
            "sender_sid": sid,
            "reason": _clean_small_text(data.get("reason"), 40) or "stopped",
        }, room=room)
        await sio.emit("screen_share_state", {"screen_share": None}, room=room)
        log.info("   screen_stop sid=%-8s room=%-18s", sid[:8], room)
    except Exception as exc:
        log.exception("screen_share_stop sid=%s: %s", sid, exc)
        await _emit_screen_error(sid, "SERVER_ERROR", "Could not stop screen sharing")


@sio.event
async def screen_share_state(sid: str, data: dict) -> None:
    try:
        room, _name = await _get_room_and_name(sid)
        if not room:
            await sio.emit("screen_share_state", {"screen_share": None}, to=sid)
            return
        await sio.emit("screen_share_state", {"screen_share": await _get_screen_state(room)}, to=sid)
    except Exception as exc:
        log.exception("screen_share_state sid=%s: %s", sid, exc)


@sio.event
async def screen_viewer_ready(sid: str, data: dict) -> None:
    """Viewer asks the active sharer to create an offer for this viewer."""
    try:
        if not isinstance(data, dict):
            data = {}
        if not await _redis_check_rate(sid, _S_MAX_SCREEN_SIGNAL_RATE, ":signal"):
            await _emit_screen_error(sid, "RATE_LIMITED", "Screen sharing signaling too fast")
            return

        room, viewer_name = await _get_room_and_name(sid)
        if not room:
            await _emit_screen_error(sid, "NOT_IN_ROOM", "Join a room before watching a screen share")
            return
        current = await _get_screen_state(room)
        if not current:
            await _emit_screen_error(sid, "NO_ACTIVE_SHARE", "No active screen share in this room")
            return
        presenter_sid = current.get("sender_sid", "")
        if presenter_sid == sid:
            return

        await sio.emit("screen_viewer_ready", {
            "viewer_sid": sid,
            "viewer_name": viewer_name,
            "stream_id": current.get("stream_id", ""),
        }, to=presenter_sid)
    except Exception as exc:
        log.exception("screen_viewer_ready sid=%s: %s", sid, exc)
        await _emit_screen_error(sid, "SERVER_ERROR", "Could not request screen share")


@sio.event
async def screen_offer(sid: str, data: dict) -> None:
    """Relay WebRTC offer from active screen sharer to one viewer."""
    try:
        if not isinstance(data, dict):
            data = {}
        if not await _redis_check_rate(sid, _S_MAX_SCREEN_SIGNAL_RATE, ":signal"):
            await _emit_screen_error(sid, "RATE_LIMITED", "Screen sharing signaling too fast")
            return

        room, name = await _get_room_and_name(sid)
        target_sid = str(data.get("to") or data.get("target_sid") or "")[:128]
        sdp = _sdp_from_data(data, "sdp")
        offer_type = _clean_webrtc_type(data.get("type"), {"offer"}, "offer")
        if not room or not target_sid or not sdp:
            await _emit_screen_error(sid, "BAD_OFFER", "screen_offer needs target_sid/to and valid SDP")
            return
        current = await _get_screen_state(room)
        if not current or current.get("sender_sid") != sid:
            await _emit_screen_error(sid, "NOT_SHARER", "Only the active screen sharer can send offers")
            return
        if not await _sid_in_room(target_sid, room):
            await _emit_screen_error(sid, "TARGET_NOT_IN_ROOM", "Target viewer is not in your room")
            return

        await sio.emit("screen_offer", {
            "from": sid,
            "from_name": name,
            "stream_id": current.get("stream_id", ""),
            "type": offer_type,
            "sdp": sdp,
        }, to=target_sid)
    except Exception as exc:
        log.exception("screen_offer sid=%s: %s", sid, exc)
        await _emit_screen_error(sid, "SERVER_ERROR", "Could not relay screen offer")


@sio.event
async def screen_answer(sid: str, data: dict) -> None:
    """Relay WebRTC answer from viewer to active screen sharer."""
    try:
        if not isinstance(data, dict):
            data = {}
        if not await _redis_check_rate(sid, _S_MAX_SCREEN_SIGNAL_RATE, ":signal"):
            await _emit_screen_error(sid, "RATE_LIMITED", "Screen sharing signaling too fast")
            return

        room, name = await _get_room_and_name(sid)
        sdp = _sdp_from_data(data, "sdp")
        answer_type = _clean_webrtc_type(data.get("type"), {"answer"}, "answer")
        if not room or not sdp:
            await _emit_screen_error(sid, "BAD_ANSWER", "screen_answer needs valid SDP")
            return
        current = await _get_screen_state(room)
        if not current:
            await _emit_screen_error(sid, "NO_ACTIVE_SHARE", "No active screen share in this room")
            return
        target_sid = str(data.get("to") or data.get("target_sid") or current.get("sender_sid") or "")[:128]
        if target_sid == sid or not await _sid_in_room(target_sid, room):
            await _emit_screen_error(sid, "TARGET_NOT_IN_ROOM", "Target sharer is not in your room")
            return

        await sio.emit("screen_answer", {
            "from": sid,
            "from_name": name,
            "stream_id": current.get("stream_id", ""),
            "type": answer_type,
            "sdp": sdp,
        }, to=target_sid)
    except Exception as exc:
        log.exception("screen_answer sid=%s: %s", sid, exc)
        await _emit_screen_error(sid, "SERVER_ERROR", "Could not relay screen answer")


@sio.event
async def screen_ice_candidate(sid: str, data: dict) -> None:
    """Relay one WebRTC ICE candidate between sharer and viewer."""
    try:
        if not isinstance(data, dict):
            data = {}
        if not await _redis_check_rate(sid, _S_MAX_SCREEN_SIGNAL_RATE, ":signal"):
            return

        room, name = await _get_room_and_name(sid)
        target_sid = str(data.get("to") or data.get("target_sid") or "")[:128]
        candidate = _clean_ice_candidate(data.get("candidate"))
        if not room or not target_sid or candidate is None:
            return
        if target_sid == sid or not await _sid_in_room(target_sid, room):
            return
        current = await _get_screen_state(room)
        stream_id = str(data.get("stream_id") or (current or {}).get("stream_id") or "")[:48]

        await sio.emit("screen_ice_candidate", {
            "from": sid,
            "from_name": name,
            "stream_id": stream_id,
            "candidate": candidate,
        }, to=target_sid)
    except Exception as exc:
        log.exception("screen_ice_candidate sid=%s: %s", sid, exc)


@sio.event
async def quality_pong(sid: str, data: dict) -> None:
    try:
        if not isinstance(data, dict):
            return
        nonce = str(data.get("nonce") or "")
        if not nonce:
            return
        state = _quality.get(sid)
        if not state:
            return
        sent_at = state["pending"].pop(nonce, None)
        if sent_at is None:
            return
        rtt_ms = (time.monotonic() - sent_at) * 1000
        state["rtts"].append(rtt_ms)
        state["cycles"].append(True)
        log.debug("quality pong  sid=%s  rtt=%.1fms", sid[:8], rtt_ms)
    except Exception as exc:
        log.exception("quality_pong sid=%s: %s", sid, exc)


@sio.event
async def msg_delivered(sid: str, data: dict) -> None:
    try:
        if not isinstance(data, dict):
            return
        msg_id = str(data.get("msg_id") or "")[:64]
        to     = str(data.get("to") or "")[:128]
        if not msg_id or not to:
            return
        # Local check first; Redis hexists only if needed and redis is available
        exists = to in _local_users
        if not exists and _redis:
            exists = bool(await _redis.hexists(_RK_PRESENCE, to))
        if exists:
            await sio.emit("msg_delivered", {"msg_id": msg_id}, to=to)
    except Exception as exc:
        log.exception("msg_delivered sid=%s: %s", sid, exc)
