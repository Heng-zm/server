"""
WalkieTalk — signaling + voice relay server
FastAPI + python-socketio (ASGI) + Redis pub/sub for multi-instance scale

Environment variables:
    SUPABASE_URL   = https://bgqeqiyfgpdvgeepignt.supabase.co
    SUPABASE_KEY   = sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6
    REDIS_URL      = redis://localhost:6379   (empty = single-instance mode)

Run locally:
    uvicorn server:socket_app --host 0.0.0.0 --port 3000 --reload

Deploy multi-instance (Render / Railway):
    Start: uvicorn server:socket_app --host 0.0.0.0 --port $PORT --workers 1
"""

import asyncio
import logging
import os
import re
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Deque

import httpx
import socketio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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

# Redis key prefixes
_RK_ROOM     = "wt:room:"
_RK_USER     = "wt:user:"
_RK_RATE     = "wt:rate:"
_RK_PRESENCE = "wt:presence"
_USER_TTL    = 3600
_RATE_TTL    = int(MSG_RATE_WINDOW * 2)

ALLOWED_MIME: frozenset[str] = frozenset({
    "audio/webm", "audio/webm;codecs=opus",
    "audio/mp4",  "audio/ogg", "audio/wav",
})

# ── Precompiled regexes (avoid recompiling on every call) ─────────────────────
_NAME_RE  = re.compile(r"[^a-z0-9_\-]")
_ROOM_RE  = re.compile(r"[^A-Z0-9_\-]")
_DEV_RE   = re.compile(r"[^a-zA-Z0-9_\-]")
_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{6}$")   # FIX 3: was recompiled every call

_start_time = time.time()

# ── Lua scripts — module-level constants (FIX 1: was rebuilt per call) ────────

# Atomic join: check capacity + register user in one Redis round-trip
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
return 1
"""

# Atomic leave: remove from room + user + presence + cleanup empty room
_LUA_LEAVE = """
local room_key = KEYS[1]
local user_key = KEYS[2]
local pres_key = KEYS[3]
local sid      = ARGV[1]
local data = redis.call('hgetall', user_key)
if #data == 0 then return {false, ''} end
local old_room, name = '', ''
for i = 1, #data, 2 do
    if data[i] == 'room' then old_room = data[i+1]
    elseif data[i] == 'name' then name = data[i+1] end
end
redis.call('delete', user_key)
redis.call('hdel', pres_key, sid)
if old_room ~= '' then
    redis.call('srem', room_key, sid)
    if redis.call('scard', room_key) == 0 then
        redis.call('del', room_key)
    end
end
return {old_room, name}
"""

# ── Shared clients ─────────────────────────────────────────────────────────────
_http:  httpx.AsyncClient | None = None
_redis                           = None

# ── Local in-memory state ──────────────────────────────────────────────────────
_local_users:     dict[str, dict]             = {}
_local_rooms:     dict[str, set]              = {}
_local_msg_times: dict[str, Deque[float]]     = {}   # FIX 8: correctly typed as deque

INSTANCE_ID = f"inst_{os.getpid()}_{int(time.time()) % 10000}"

# ── Health ping cache (FIX 10) ────────────────────────────────────────────────
_last_ping_ok:   bool  = False
_last_ping_time: float = 0.0
_PING_CACHE_TTL: float = 10.0


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _http, _redis

    # HTTP client — conservative pool for single upstream host (FIX 11)
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
    yield

    if _redis:
        try:
            all_presence = await _redis.hgetall(_RK_PRESENCE)
            mine = [s for s, iid in all_presence.items() if iid == INSTANCE_ID]
            if mine:
                await _redis.hdel(_RK_PRESENCE, *mine)
                for sid in mine:
                    await _redis_leave(sid, known_room=None)
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
        # FIX 18: higher ping_interval reduces churn on mobile backgrounding
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
    allow_credentials=False,   # FIX 16: explicit false skips a middleware check
)
socket_app = socketio.ASGIApp(sio, app)


# ── Redis helpers ─────────────────────────────────────────────────────────────

async def _redis_atomic_join(sid: str, room: str, name: str) -> bool:
    """Atomic capacity check + join via Lua. Returns True if admitted."""
    if not _redis:
        if len(_local_rooms.get(room, set())) >= MAX_ROOM_SIZE:
            return False
        _local_users[sid] = {"room": room, "name": name, "joined_at": time.time()}
        _local_rooms.setdefault(room, set()).add(sid)
        return True

    result = await _redis.eval(
        _LUA_JOIN, 3,
        _RK_ROOM + room, _RK_USER + sid, _RK_PRESENCE,
        sid, room, name,
        str(MAX_ROOM_SIZE), f"{time.time():.3f}", INSTANCE_ID, str(_USER_TTL),
    )
    admitted = bool(result)
    if admitted:
        _local_users[sid] = {"room": room, "name": name, "joined_at": time.time()}
        _local_rooms.setdefault(room, set()).add(sid)
    return admitted


async def _redis_leave(sid: str, known_room: str | None) -> tuple[str | None, str]:
    """
    FIX 5+9: Single Lua round-trip for leave — removes user, presence,
    and cleans empty room atomically. known_room avoids HGETALL when local
    state already has the room.
    """
    if not _redis:
        return None, sid[:6]

    # If we already know the room, skip the HGETALL inside Lua
    room_key = _RK_ROOM + (known_room or "")
    result = await _redis.eval(
        _LUA_LEAVE, 3,
        room_key, _RK_USER + sid, _RK_PRESENCE,
        sid,
    )
    if not result or not result[0]:
        return known_room, sid[:6]
    return result[0] or known_room, result[1] or sid[:6]


async def _redis_room_members(room: str) -> list[dict]:
    """FIX 6: pipeline SMEMBERS + batch HGET in one round-trip."""
    if not _redis:
        return _local_room_members(room)
    sids = await _redis.smembers(_RK_ROOM + room)
    if not sids:
        return []
    pipe = _redis.pipeline(transaction=False)   # no MULTI/EXEC — faster for reads
    for sid in sids:
        pipe.hget(_RK_USER + sid, "name")
    names = await pipe.execute()
    return [{"sid": s, "name": n} for s, n in zip(sids, names) if n]


async def _redis_check_rate(sid: str) -> bool:
    """
    FIX 12: Check count BEFORE adding so we never need a zrem undo.
    Saves one Redis round-trip on every denied message.
    """
    if not _redis:
        return _local_check_rate(sid)

    now    = time.time()
    cutoff = now - MSG_RATE_WINDOW
    key    = _RK_RATE + sid
    member = f"{now:.6f}:{sid}"

    pipe = _redis.pipeline(transaction=False)
    pipe.zremrangebyscore(key, "-inf", cutoff)   # evict expired
    pipe.zcard(key)                               # count BEFORE adding
    pipe.expire(key, _RATE_TTL)
    results = await pipe.execute()
    current_count: int = results[1]

    if current_count >= MAX_MSG_RATE:
        return False   # denied — no zadd needed, no undo needed

    # Allowed — now record this attempt
    await _redis.zadd(key, {member: now})
    return True


async def _redis_sid_exists(sid: str) -> bool:
    if _redis:
        return bool(await _redis.hexists(_RK_PRESENCE, sid))
    return sid in _local_users


# ── Unified leave (local + Redis in one call) ─────────────────────────────────
async def _leave_room(sid: str) -> tuple[str | None, str]:
    # Pull from local first — avoids HGETALL in Lua when possible (FIX 9)
    info     = _local_users.get(sid)
    known    = info.get("room") if info else None
    name_loc = info.get("name", sid[:6]) if info else sid[:6]

    # Clean local state
    if info:
        if known:
            room_set = _local_rooms.get(known)
            if room_set is not None:
                room_set.discard(sid)
                if not room_set:
                    del _local_rooms[known]
        info["room"] = None
    _local_users.pop(sid, None)
    _local_msg_times.pop(sid, None)

    if _redis:
        r_room, r_name = await _redis_leave(sid, known_room=known)
        return r_room or known, r_name or name_loc

    return known, name_loc


# ── Local helpers ─────────────────────────────────────────────────────────────

def _local_room_members(room: str) -> list[dict]:
    snap = frozenset(_local_rooms.get(room, set()))
    return [{"sid": s, "name": _local_users[s]["name"]}
            for s in snap if s in _local_users]


def _local_check_rate(sid: str) -> bool:
    now    = time.time()
    cutoff = now - MSG_RATE_WINDOW
    times  = _local_msg_times.get(sid)
    if times is None:
        times = deque()
        _local_msg_times[sid] = times
    while times and times[0] <= cutoff:
        times.popleft()
    if len(times) >= MAX_MSG_RATE:
        return False
    times.append(now)
    return True


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
    return s if _COLOR_RE.match(s) else "#007aff"   # FIX 3: uses precompiled pattern


# ── HTTP endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    global _last_ping_ok, _last_ping_time
    # FIX 10: cache Redis ping result — don't hit Redis on every health check
    now = time.time()
    if _redis and (now - _last_ping_time) > _PING_CACHE_TTL:
        try:
            await _redis.ping()
            _last_ping_ok = True
        except Exception:
            _last_ping_ok = False
        _last_ping_time = now

    return JSONResponse({
        "status":       "ok",
        "instance":     INSTANCE_ID,
        "connections":  len(_local_users),
        "rooms_local":  {k: len(v) for k, v in _local_rooms.items()},
        "redis":        _last_ping_ok if _redis else None,
        "uptime_s":     round(now - _start_time),
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
            "order":  "created_at.asc",
            "select": "id,device_id,name,channel,lat,lng,radius,color,auto_join,created_by",
        })
        if not r.is_success:
            log.error("Supabase GET failed %s: %s", r.status_code, r.text[:200])
            return JSONResponse({"error": "upstream error", "status": r.status_code, "detail": r.text}, status_code=502)
        return JSONResponse(r.json())
    except Exception as e:
        log.exception("get_zones: %s", e)
        return JSONResponse({"error": "server error", "detail": str(e)}, status_code=500)


@app.post("/zones")
async def upsert_zone(request: Request) -> JSONResponse:
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

    payload = {
        "id": zone_id, "device_id": device_id,
        "name": name or channel, "channel": channel,
        "lat": lat, "lng": lng, "radius": radius,
        "color": color, "auto_join": auto_join,
        "created_by": created_by[:32],
    }

    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    try:
        r = await _http.post(
            "/rest/v1/geo_zones", json=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
        if not r.is_success:
            log.error("Supabase upsert failed %s: %s", r.status_code, r.text[:200])
            return JSONResponse({"error": "upstream error", "status": r.status_code, "detail": r.text}, status_code=502)

        await sio.emit("zone_upserted", {
            "id": zone_id, "device_id": device_id,
            "name": payload["name"], "channel": channel,
            "lat": lat, "lng": lng, "radius": radius,
            "color": color, "auto_join": auto_join,
            "created_by": created_by,
        })
        return JSONResponse({"ok": True})
    except Exception as e:
        log.exception("upsert_zone: %s", e)
        return JSONResponse({"error": "server error", "detail": str(e)}, status_code=500)


@app.delete("/zones/{zone_id}")
async def delete_zone(zone_id: str, request: Request) -> JSONResponse:
    device_id = _sanitize_device(request.query_params.get("device_id", ""))
    zone_id   = _sanitize_device(zone_id)
    if not device_id or not zone_id:
        return JSONResponse({"error": "device_id and zone_id required"}, status_code=400)
    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    try:
        r = await _http.delete("/rest/v1/geo_zones",
            params={"id": f"eq.{zone_id}", "device_id": f"eq.{device_id}"})
        if not r.is_success:
            log.error("Supabase DELETE failed %s: %s", r.status_code, r.text[:200])
            return JSONResponse({"error": "upstream error", "status": r.status_code, "detail": r.text}, status_code=502)
        await sio.emit("zone_deleted", {"id": zone_id, "device_id": device_id})
        return JSONResponse({"ok": True})
    except Exception as e:
        log.exception("delete_zone: %s", e)
        return JSONResponse({"error": "server error", "detail": str(e)}, status_code=500)


# ── Socket events ──────────────────────────────────────────────────────────────

@sio.event
async def connect(sid: str, environ: dict) -> None:
    pass


@sio.event
async def disconnect(sid: str) -> None:
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

        old_room, _ = await _leave_room(sid)
        if old_room and old_room != room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)

        admitted = await _redis_atomic_join(sid, room, name)
        if not admitted:
            await sio.emit("error", {"code": "ROOM_FULL", "msg": f"Room full ({MAX_ROOM_SIZE} max)"}, to=sid)
            log.warning("Room %s full — rejected %s", room, sid)
            return

        await sio.enter_room(sid, room)

        # FIX 17: _redis_room_members is an async function and handles the local fallback internally 
        # when _redis is None. No need for asyncio.coroutine.
        members = await _redis_room_members(room)
        
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
        if not info:
            return
        old_name     = info["name"]
        info["name"] = new_name
        room         = info.get("room")
        if _redis:
            await _redis.hset(_RK_USER + sid, "name", new_name)
        if room:
            await sio.emit("peer_name_updated", {"sid": sid, "name": new_name}, room=room, skip_sid=sid)
        log.info("   rename @%s -> @%s", old_name, new_name)
    except Exception as exc:
        log.exception("update_name sid=%s: %s", sid, exc)


@sio.event
async def voice_message(sid: str, data: dict) -> None:
    try:
        # FIX 4+7: local lookup first — only hit Redis on cache miss
        info = _local_users.get(sid)
        room = info.get("room") if info else None
        if not room:
            # Fallback: Redis knows if this sid joined on another instance
            if _redis:
                room = await _redis.hget(_RK_USER + sid, "room")
            if not room:
                return

        # Rate check (Redis pipeline, FIX 12)
        if not await _redis_check_rate(sid):
            await sio.emit("error", {"code": "RATE_LIMITED", "msg": "Sending too fast"}, to=sid)
            return

        name   = info.get("name", sid[:6]) if info else sid[:6]
        audio  = data.get("audio", "")
        if not audio:
            return

        audio_len = len(audio)
        if audio_len > MAX_AUDIO_BYTES:
            await sio.emit("error", {"code": "MSG_TOO_LARGE", "msg": "Audio too large"}, to=sid)
            return

        mime = str(data.get("mime", "audio/webm"))
        if mime not in ALLOWED_MIME:
            mime = "audio/webm"

        msg_id = str(data.get("msg_id", ""))[:64]
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
async def msg_delivered(sid: str, data: dict) -> None:
    try:
        msg_id = str(data.get("msg_id", ""))[:64]
        to     = str(data.get("to", ""))
        if not msg_id or not to:
            return
        # Fast local check first, Redis fallback
        exists = (to in _local_users) or (bool(await _redis.hexists(_RK_PRESENCE, to)) if _redis else False)
        if exists:
            await sio.emit("msg_delivered", {"msg_id": msg_id}, to=to)
    except Exception as exc:
        log.exception("msg_delivered sid=%s: %s", sid, exc)
