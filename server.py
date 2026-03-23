"""
WalkieTalk — signaling + voice relay server
FastAPI + python-socketio (ASGI) + Redis pub/sub for multi-instance scale

Environment variables:
    SUPABASE_URL        = https://bgqeqiyfgpdvgeepignt.supabase.co
    SUPABASE_KEY        = sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6
    REDIS_URL           = redis://localhost:6379   (empty = single-instance mode)
    RENDER_EXTERNAL_URL = set automatically by Render — used for self-ping keepalive
                          (set SERVER_URL manually on other platforms)

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
SUPABASE_URL = os.environ.get("SUPABASE_URL",
    "https://bgqeqiyfgpdvgeepignt.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY",
    "sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6")
REDIS_URL    = os.environ.get("REDIS_URL", "")

KEEPALIVE_URL = (
    os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    or os.environ.get("SERVER_URL", "").rstrip("/")
)

_SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
}

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_ROOM_SIZE   = 20
MAX_NAME_LEN    = 32
MAX_ROOM_LEN    = 40
MAX_AUDIO_BYTES = 8_000_000
MAX_DURATION    = 65.0
MAX_MSG_RATE    = 4
MSG_RATE_WINDOW = 10.0
MAX_CHUNK_BYTES = 200_000        # per live-voice chunk
MAX_CHUNK_RATE  = 8              # chunks per MSG_RATE_WINDOW in live mode

ZONE_TTL_SECS: int = 5 * 3600   # 5 hours in seconds (avoids datetime import)

# Redis key prefixes
_RK_ROOM     = "wt:room:"
_RK_USER     = "wt:user:"
_RK_RATE     = "wt:rate:"
_RK_PRESENCE = "wt:presence"
_USER_TTL    = 3600
_RATE_TTL    = int(MSG_RATE_WINDOW * 2)

# Pre-computed constant strings — avoids repeated str() on the hot path
_S_MAX_MSG_RATE  = str(MAX_MSG_RATE)
_S_MAX_CHUNK_RATE = str(MAX_CHUNK_RATE)
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
if not room then return {nil, nil} end
redis.call('del', user_key)
redis.call('hdel', pres_key, sid)
if room ~= '' then
    local rk = room_pfx .. room
    redis.call('srem', rk, sid)
    if redis.call('scard', rk) == 0 then
        redis.call('del', rk)
    end
end
return {room, name or ''}
"""

# ── Shared clients ─────────────────────────────────────────────────────────────
_http:  httpx.AsyncClient | None = None
_redis                           = None

# ── Local in-memory state ──────────────────────────────────────────────────────
_local_users:     dict[str, dict]         = {}
_local_rooms:     dict[str, set]          = {}
_local_msg_times: dict[str, deque[float]] = {}   # PTT rate
_local_chunk_times: dict[str, deque[float]] = {}  # live chunk rate

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
            nonce   = f"{sid}_{cycle}"
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
            log.warning("Zone expiry task error: %s", exc)


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
                log.info("Keepalive ping  status=%d  uptime=%ss",
                         r.status_code,
                         r.json().get("uptime_s", "?") if r.is_success else "?")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Keepalive ping failed: %s", exc)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _http, _redis

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

    await _http.aclose()
    log.info("WalkieTalk stopped  instance=%s", INSTANCE_ID)


# ── Socket.IO ──────────────────────────────────────────────────────────────────
def _build_sio() -> socketio.AsyncServer:
    common = dict(
        async_mode="asgi",
        cors_allowed_origins="*",
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
    allow_origins=["*"],
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

    result = await _redis.eval(
        _LUA_JOIN, 3,
        _RK_ROOM + room, _RK_USER + sid, _RK_PRESENCE,
        sid, room, name,
        _S_MAX_ROOM, f"{now_ts:.3f}", INSTANCE_ID, _S_USER_TTL,
    )
    admitted = bool(result)
    if admitted:
        # Mirror in local state — float consistent with local path
        _local_users[sid] = {"room": room, "name": name, "joined_at": now_ts}
        _local_rooms.setdefault(room, set()).add(sid)
    return admitted


async def _redis_leave(sid: str, known_room: str | None) -> tuple[str | None, str]:
    if not _redis:
        return None, sid[:6]

    result = await _redis.eval(
        _LUA_LEAVE, 2,
        _RK_USER + sid, _RK_PRESENCE,
        sid, _RK_ROOM,
    )
    if not result or result[0] is None:
        return known_room, sid[:6]
    return result[0] or known_room, result[1] or sid[:6]


async def _redis_room_members(room: str) -> list[dict]:
    if not _redis:
        return _local_room_members(room)
    sids = await _redis.smembers(_RK_ROOM + room)
    if not sids:
        return []
    sids = list(sids)[:MAX_ROOM_SIZE]
    pipe = _redis.pipeline(transaction=False)
    for sid in sids:
        pipe.hget(_RK_USER + sid, "name")
    names = await pipe.execute()
    return [{"sid": s, "name": n} for s, n in zip(sids, names) if n]


async def _redis_check_rate(sid: str, limit_str: str | None = None, key_suffix: str = "") -> bool:
    """
    Unified rate checker for both PTT messages and live chunks.
    key_suffix="" → PTT key, key_suffix=":live" → chunk key.
    limit_str defaults to _S_MAX_MSG_RATE.
    """
    _limit = limit_str or _S_MAX_MSG_RATE
    if not _redis:
        # Local fallback — uses the appropriate deque based on suffix
        return _local_check_rate(sid, live=(key_suffix == ":live"))

    now    = time.time()
    cutoff = now - MSG_RATE_WINDOW
    key    = _RK_RATE + sid + key_suffix
    member = f"{now:.6f}"   # key already scoped to sid — no suffix needed

    result = await _redis.eval(
        _LUA_RATE, 1,
        key,
        f"{cutoff:.6f}", member, f"{now:.6f}",
        _limit, _S_RATE_TTL,
    )
    return bool(result)


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

    if _redis:
        # Only fetch joined_at if we know the user was in a room (avoids wasted round-trip)
        if not joined and known:
            try:
                joined = await _redis.hget(_RK_USER + sid, "joined_at")
            except Exception:
                joined = None
        r_room, r_name = await _redis_leave(sid, known_room=known)
        final_room = r_room or known
        final_name = r_name or name_loc
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


def _local_check_rate(sid: str, live: bool = False) -> bool:
    """Single local rate checker for both PTT (live=False) and chunk (live=True)."""
    now    = time.time()
    cutoff = now - MSG_RATE_WINDOW
    store  = _local_chunk_times if live else _local_msg_times
    limit  = MAX_CHUNK_RATE if live else MAX_MSG_RATE
    times  = store.get(sid)
    if times is None:
        times = deque(maxlen=limit + 1)
        store[sid] = times
    while times and times[0] <= cutoff:
        times.popleft()
    if len(times) >= limit:
        return False
    times.append(now)
    return True


# ── Room+name lookup — shared hot-path helper ─────────────────────────────────
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
        vals = await _redis.hmget(_RK_USER + sid, "room", "name")
        return vals[0], (vals[1] or sid[:6])
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


# ── HTTP endpoints ─────────────────────────────────────────────────────────────

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

    return JSONResponse({
        "status":      "ok",
        "instance":    INSTANCE_ID,
        "connections": conn,
        "rooms_local": rooms,
        "redis":       _last_ping_ok if _redis else None,
        "uptime_s":    round(now - _start_time),
    })


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
        room = _sanitize_room(data.get("room", ""))
        name = _sanitize_name(data.get("name", ""), sid[:6])
        if not room:
            return

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
        # joined_at already set as float in _redis_atomic_join — no overwrite needed

        members = await _redis_room_members(room) if _redis else _local_room_members(room)
        await asyncio.gather(
            sio.emit("peer_joined", {"sid": sid, "name": name}, room=room, skip_sid=sid),
            sio.emit("room_state",  {"members": members}, to=sid),
        )
        log.info("[+] %-24s @%-16s  room=%-20s  n=%d", sid, name, room, len(members))

    except Exception as exc:
        log.exception("join_room sid=%s: %s", sid, exc)


@sio.event
async def leave_room_event(sid: str, data: dict) -> None:
    try:
        old_room, name = await _leave_room(sid)
        if old_room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)
    except Exception as exc:
        log.exception("leave_room_event sid=%s: %s", sid, exc)


@sio.event
async def update_name(sid: str, data: dict) -> None:
    try:
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
            await sio.emit("peer_name_updated", {"sid": sid, "name": new_name}, room=room, skip_sid=sid)
        log.info("   rename @%s -> @%s", old_name, new_name)
    except Exception as exc:
        log.exception("update_name sid=%s: %s", sid, exc)


@sio.event
async def voice_message(sid: str, data: dict) -> None:
    try:
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

    except Exception as exc:
        log.exception("voice_message sid=%s: %s", sid, exc)


@sio.event
async def voice_chunk(sid: str, data: dict) -> None:
    """Live voice streaming — relay a single audio chunk to the room immediately."""
    try:
        room, name = await _get_room_and_name(sid)
        if not room:
            return

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
async def quality_pong(sid: str, data: dict) -> None:
    try:
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
