"""
WalkieTalk — signaling + voice relay server
FastAPI + python-socketio (ASGI) + Redis pub/sub for multi-instance scale

Environment variables:
    SUPABASE_URL   = https://bgqeqiyfgpdvgeepignt.supabase.co
    SUPABASE_KEY   = sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6
    REDIS_URL      = redis://localhost:6379          # or Upstash / Redis Cloud URL
                     set to empty string to run single-instance (no Redis)

Run locally (single instance, no Redis):
    uvicorn server:socket_app --host 0.0.0.0 --port 3000 --reload

Run locally with Redis:
    REDIS_URL=redis://localhost:6379 uvicorn server:socket_app --host 0.0.0.0 --port 3000

Deploy multi-instance on Render / Railway:
    Start command: uvicorn server:socket_app --host 0.0.0.0 --port $PORT --workers 1
    (set REDIS_URL env var to your Redis instance)

How multi-instance works:
    ┌─────────────┐     pub/sub      ┌─────────────┐
    │  Instance A │ ◄──── Redis ────► │  Instance B │
    │  Client 1,2 │                  │  Client 3,4 │
    └─────────────┘                  └─────────────┘
    sio.emit() on A publishes to Redis → B fans out to its local clients.
    Room state, rate limits, and presence all live in Redis (shared across instances).
"""

import asyncio
import logging
import os
import re
import time
from contextlib import asynccontextmanager

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

# ── Config from environment ───────────────────────────────────────────────────
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://bgqeqiyfgpdvgeepignt.supabase.co",
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6",
)
REDIS_URL = os.environ.get("REDIS_URL", "")   # empty = single-instance mode

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

# Redis key prefixes — all WalkieTalk data is namespaced
_RK_ROOM     = "wt:room:"      # SET  — room members:  wt:room:<room_id>  → {sid, ...}
_RK_USER     = "wt:user:"      # HASH — user record:   wt:user:<sid>      → {room, name, joined_at}
_RK_RATE     = "wt:rate:"      # ZSET — rate limiter:  wt:rate:<sid>      → timestamps (score=ts)
_RK_PRESENCE = "wt:presence"   # HASH — global map:    sid → instance_id
_USER_TTL    = 3600            # seconds — auto-expire stale user records after 1h
_RATE_TTL    = 60              # seconds — rate limit keys expire after 1 window

ALLOWED_MIME: frozenset[str] = frozenset({
    "audio/webm",
    "audio/webm;codecs=opus",
    "audio/mp4",
    "audio/ogg",
    "audio/wav",
})

_NAME_RE   = re.compile(r"[^a-z0-9_\-]")
_ROOM_RE   = re.compile(r"[^A-Z0-9_\-]")
_DEVICE_RE = re.compile(r"[^a-zA-Z0-9_\-]")
_start_time = time.time()

# ── Shared clients (set in lifespan) ──────────────────────────────────────────
_http:  httpx.AsyncClient | None = None
_redis: "aioredis.Redis | None"  = None   # type: ignore[name-defined]

# ── Local in-memory fallback (used when REDIS_URL is empty) ───────────────────
# Also used for msg_times when Redis is available but we want O(1) rate check
_local_users:     dict[str, dict] = {}
_local_rooms:     dict[str, set]  = {}
_local_msg_times: dict[str, list] = {}   # sid -> list[float]

INSTANCE_ID = f"inst_{os.getpid()}_{int(time.time()) % 10000}"


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _http, _redis

    # HTTP client for Supabase
    _http = httpx.AsyncClient(
        base_url=SUPABASE_URL,
        headers=_SB_HEADERS,
        timeout=10.0,
    )

    # Redis client (optional)
    if REDIS_URL:
        try:
            import aioredis
            _redis = await aioredis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            await _redis.ping()
            log.info("Redis connected  url=%s  instance=%s", REDIS_URL[:40], INSTANCE_ID)
        except Exception as exc:
            log.warning("Redis unavailable (%s) — falling back to single-instance mode", exc)
            _redis = None
    else:
        log.info("REDIS_URL not set — running in single-instance mode  instance=%s", INSTANCE_ID)

    log.info("WalkieTalk started  pid=%d  supabase=%s", os.getpid(), SUPABASE_URL[:40])

    yield

    # Cleanup: remove this instance's presence entries from Redis
    if _redis:
        try:
            # Remove all sids belonging to this instance from Redis presence + rooms
            all_presence = await _redis.hgetall(_RK_PRESENCE)
            mine = [sid for sid, iid in all_presence.items() if iid == INSTANCE_ID]
            if mine:
                # Batch remove presence entries
                await _redis.hdel(_RK_PRESENCE, *mine)
                # Clean up room memberships for each departed sid
                for sid in mine:
                    await _redis_leave_room(sid)
            log.info(
                "WalkieTalk stopping  instance=%s  cleaned %d stale presences",
                INSTANCE_ID, len(mine),
            )
        except Exception as exc:
            log.warning("Redis cleanup error on shutdown: %s", exc)
        finally:
            await _redis.aclose()   # always close after cleanup, not before

    await _http.aclose()
    log.info("WalkieTalk stopped  instance=%s  local_connections=%d", INSTANCE_ID, len(_local_users))


# ── Socket.IO — with Redis manager if available, memory manager if not ────────
def _build_sio() -> socketio.AsyncServer:
    """
    Build the AsyncServer. We always start with the memory manager here;
    the Redis manager is swapped in after Redis connects in lifespan
    (python-socketio requires the manager at construction time, so we use
    a deferred pattern: build with memory, replace if Redis available).

    For production: set REDIS_URL and the AsyncRedisManager handles
    cross-instance pub/sub transparently. sio.emit() calls on any instance
    publish to the Redis channel and every instance fans out to its locals.
    """
    if REDIS_URL:
        try:
            mgr = socketio.AsyncRedisManager(REDIS_URL, channel="walkie_sio")
            log.info("Using AsyncRedisManager for socket pub/sub")
            return socketio.AsyncServer(
                client_manager=mgr,
                async_mode="asgi",
                cors_allowed_origins="*",
                ping_timeout=20,
                ping_interval=15,
                max_http_buffer_size=MAX_AUDIO_BYTES + 512_000,
                logger=False,
                engineio_logger=False,
            )
        except Exception as exc:
            log.warning("AsyncRedisManager init failed (%s) — using memory manager", exc)

    return socketio.AsyncServer(
        async_mode="asgi",
        cors_allowed_origins="*",
        ping_timeout=20,
        ping_interval=15,
        max_http_buffer_size=MAX_AUDIO_BYTES + 512_000,
        logger=False,
        engineio_logger=False,
    )


sio = _build_sio()

app = FastAPI(title="WalkieTalk", docs_url=None, redoc_url=None, lifespan=_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
socket_app = socketio.ASGIApp(sio, app)


# ── Redis state helpers ───────────────────────────────────────────────────────

async def _redis_join_room(sid: str, room: str, name: str) -> None:
    """Register user in Redis room set + user hash."""
    if not _redis:
        return
    pipe = _redis.pipeline()
    pipe.sadd(_RK_ROOM + room, sid)
    pipe.hset(_RK_USER + sid, mapping={
        "room":      room,
        "name":      name,
        "joined_at": str(time.time()),
    })
    pipe.expire(_RK_USER + sid, _USER_TTL)
    pipe.hset(_RK_PRESENCE, sid, INSTANCE_ID)
    await pipe.execute()


async def _redis_atomic_join(sid: str, room: str, name: str) -> bool:
    """
    Atomically check capacity and add sid to room using a Lua script.
    Returns True if admitted, False if room is full.
    Falls back to non-atomic check when Redis is unavailable.
    """
    if not _redis:
        # Single-instance fallback — non-atomic but acceptable
        if len(_local_rooms.get(room, set())) >= MAX_ROOM_SIZE:
            return False
        _local_users[sid] = {"room": room, "name": name, "joined_at": time.time()}
        _local_rooms.setdefault(room, set()).add(sid)
        return True

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
if cur >= max_size then
    return 0
end
redis.call('sadd', room_key, sid)
redis.call('hset', user_key, 'room', room, 'name', name, 'joined_at', now)
redis.call('expire', user_key, ttl)
redis.call('hset', pres_key, sid, inst)
return 1
"""
    result = await _redis.eval(
        _LUA_JOIN,
        3,
        _RK_ROOM + room,
        _RK_USER + sid,
        _RK_PRESENCE,
        sid, room, name,
        str(MAX_ROOM_SIZE), str(time.time()), INSTANCE_ID, str(_USER_TTL),
    )
    admitted = bool(result)
    if admitted:
        # Mirror to local state for fast path lookups
        _local_users[sid] = {"room": room, "name": name, "joined_at": time.time()}
        _local_rooms.setdefault(room, set()).add(sid)
    return admitted


async def _redis_leave_room(sid: str) -> tuple[str | None, str]:
    """Remove user from Redis. Returns (old_room, name)."""
    if not _redis:
        return None, sid[:6]
    data = await _redis.hgetall(_RK_USER + sid)
    if not data:
        return None, sid[:6]
    old_room = data.get("room")
    name     = data.get("name", sid[:6])
    pipe = _redis.pipeline()
    if old_room:
        pipe.srem(_RK_ROOM + old_room, sid)
    pipe.delete(_RK_USER + sid)
    pipe.hdel(_RK_PRESENCE, sid)
    await pipe.execute()
    # Clean up empty room key separately — can't use EVAL inside pipeline with aioredis
    if old_room:
        remaining = await _redis.scard(_RK_ROOM + old_room)
        if remaining == 0:
            await _redis.delete(_RK_ROOM + old_room)
    return old_room, name


async def _redis_room_size(room: str) -> int:
    if not _redis:
        return len(_local_rooms.get(room, set()))
    return await _redis.scard(_RK_ROOM + room)


async def _redis_room_members(room: str) -> list[dict]:
    """Get [{sid, name}] for all members of a room — cross-instance."""
    if not _redis:
        return _local_room_members(room)
    sids = await _redis.smembers(_RK_ROOM + room)
    members = []
    if sids:
        pipe = _redis.pipeline()
        for sid in sids:
            pipe.hget(_RK_USER + sid, "name")
        names = await pipe.execute()
        for sid, name in zip(sids, names):
            if name:
                members.append({"sid": sid, "name": name})
    return members


async def _redis_get_user_room(sid: str) -> str | None:
    """Look up which room a sid is in — checks Redis first, local fallback."""
    if _redis:
        return await _redis.hget(_RK_USER + sid, "room")
    return (_local_users.get(sid) or {}).get("room")


async def _redis_check_rate(sid: str) -> bool:
    """
    Distributed sliding-window rate limiter using Redis sorted sets.
    Uses wall-clock time (time.time) so scores are consistent across instances.
    Each entry has a unique member key to prevent deduplication collisions.
    Falls back to in-memory deque when Redis unavailable.
    """
    if not _redis:
        return _local_check_rate(sid)

    now    = time.time()                                # wall-clock — consistent across instances
    cutoff = now - MSG_RATE_WINDOW
    key    = _RK_RATE + sid
    member = f"{now:.6f}:{sid}"                        # unique per attempt — no collision

    pipe = _redis.pipeline()
    pipe.zremrangebyscore(key, "-inf", cutoff)          # evict expired entries
    pipe.zadd(key, {member: now})                       # record this attempt
    pipe.zcard(key)                                     # count remaining in window
    pipe.expire(key, int(MSG_RATE_WINDOW * 2))          # auto-expire the key
    results = await pipe.execute()
    count: int = results[2]

    if count > MAX_MSG_RATE:
        await _redis.zrem(key, member)                  # undo — message is denied
        return False
    return True


async def _redis_sid_exists(sid: str) -> bool:
    """Check if a sid is connected anywhere in the cluster."""
    if _redis:
        return bool(await _redis.hexists(_RK_PRESENCE, sid))
    return sid in _local_users


# ── Local (non-Redis) state helpers ──────────────────────────────────────────

def _local_room_members(room: str) -> list[dict]:
    snapshot = frozenset(_local_rooms.get(room, set()))
    return [
        {"sid": sid, "name": _local_users[sid]["name"]}
        for sid in snapshot
        if sid in _local_users
    ]


def _local_leave_room(sid: str) -> tuple[str | None, str]:
    info = _local_users.get(sid)
    if not info:
        return None, sid[:6]
    old_room = info.get("room")
    name     = info.get("name", sid[:6])
    if old_room:
        room_set = _local_rooms.get(old_room)
        if room_set is not None:
            room_set.discard(sid)
            if not room_set:
                del _local_rooms[old_room]
        info["room"] = None
    return old_room, name


def _local_check_rate(sid: str) -> bool:
    from collections import deque as _dq
    now    = time.time()                               # wall-clock, consistent with Redis path
    cutoff = now - MSG_RATE_WINDOW
    raw    = _local_msg_times.get(sid)
    if raw is None:
        times: "deque[float]" = _dq()
        _local_msg_times[sid] = times                  # type: ignore[assignment]
    else:
        times = raw                                    # type: ignore[assignment]
    while times and times[0] <= cutoff:
        times.popleft()                                # O(1) on deque, not O(n) list.pop(0)
    if len(times) >= MAX_MSG_RATE:
        return False
    times.append(now)
    return True


# ── Unified state helpers (pick Redis or local automatically) ─────────────────

async def _join_room(sid: str, room: str, name: str) -> None:
    """Non-atomic join used only for reconnect/internal paths. Use _redis_atomic_join in join_room event."""
    _local_users[sid] = {"room": room, "name": name, "joined_at": time.time()}
    _local_rooms.setdefault(room, set()).add(sid)
    await _redis_join_room(sid, room, name)


async def _leave_room(sid: str) -> tuple[str | None, str]:
    old_room, name = _local_leave_room(sid)
    if _redis:
        r_room, r_name = await _redis_leave_room(sid)
        if not old_room:
            old_room, name = r_room, r_name
    _local_users.pop(sid, None)
    _local_msg_times.pop(sid, None)
    return old_room, name


async def _room_members(room: str) -> list[dict]:
    return await _redis_room_members(room) if _redis else _local_room_members(room)


async def _check_rate(sid: str) -> bool:
    return await _redis_check_rate(sid)


# ── HTTP endpoints ────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    redis_ok = False
    if _redis:
        try:
            await _redis.ping()
            redis_ok = True
        except Exception:
            pass
    return JSONResponse({
        "status":        "ok",
        "instance":      INSTANCE_ID,
        "connections":   len(_local_users),
        "rooms_local":   {k: len(v) for k, v in _local_rooms.items()},
        "redis":         redis_ok,
        "redis_url":     REDIS_URL[:30] + "..." if len(REDIS_URL) > 30 else REDIS_URL,
        "supabase_url":  SUPABASE_URL[:40],
        "uptime_s":      round(time.time() - _start_time),
    })


@app.get("/zones/ping")
async def zones_ping() -> JSONResponse:
    if _http is None:
        return JSONResponse({"ok": False, "error": "http client not ready"}, status_code=503)
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
        r = await _http.get(
            "/rest/v1/geo_zones",
            params={"order": "created_at.asc",
                    "select": "id,device_id,name,channel,lat,lng,radius,color,auto_join,created_by"},
        )
        if not r.is_success:
            log.error("Supabase GET failed status=%s body=%s", r.status_code, r.text)
            return JSONResponse({"error": "upstream error", "status": r.status_code, "detail": r.text}, status_code=502)
        return JSONResponse(r.json())
    except Exception as e:
        log.exception("get_zones error: %s", e)
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
            log.error("Supabase upsert failed status=%s body=%s", r.status_code, r.text)
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
        log.exception("upsert_zone error: %s", e)
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
        r = await _http.delete(
            "/rest/v1/geo_zones",
            params={"id": f"eq.{zone_id}", "device_id": f"eq.{device_id}"},
        )
        if not r.is_success:
            log.error("Supabase DELETE failed status=%s body=%s", r.status_code, r.text)
            return JSONResponse({"error": "upstream error", "status": r.status_code, "detail": r.text}, status_code=502)
        await sio.emit("zone_deleted", {"id": zone_id, "device_id": device_id})
        return JSONResponse({"ok": True})
    except Exception as e:
        log.exception("delete_zone error: %s", e)
        return JSONResponse({"error": "server error", "detail": str(e)}, status_code=500)


# ── Sanitizers ────────────────────────────────────────────────────────────────

def _sanitize_name(raw: str, fallback: str) -> str:
    cleaned = _NAME_RE.sub("", (raw or "").strip().lower().replace(" ", "_"))
    return cleaned[:MAX_NAME_LEN] or fallback[:MAX_NAME_LEN]

def _sanitize_room(raw: str) -> str:
    return _ROOM_RE.sub("", (raw or "").strip().upper())[:MAX_ROOM_LEN]

def _sanitize_device(raw: str) -> str:
    return _DEVICE_RE.sub("", (raw or "").strip())[:128]

def _validate_color(raw: object) -> str:
    s = str(raw or "").strip()
    return s if re.match(r"^#[0-9a-fA-F]{6}$", s) else "#007aff"


# ── Socket events ─────────────────────────────────────────────────────────────

@sio.event
async def connect(sid: str, environ: dict) -> None:
    pass  # meaningful logging happens in join_room


@sio.event
async def disconnect(sid: str) -> None:
    old_room: str | None = None
    name: str | None     = None
    try:
        old_room, name = await _leave_room(sid)
    except Exception as exc:
        log.exception("_leave_room error on disconnect sid=%s: %s", sid, exc)

    log.info("[-] %-24s @%-16s left %s", sid, name or "?", old_room or "(no room)")
    if old_room and name:
        await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)


@sio.event
async def join_room(sid: str, data: dict) -> None:
    try:
        room = _sanitize_room(data.get("room", ""))
        name = _sanitize_name(data.get("name", ""), sid[:6])
        if not room:
            return

        # Leave current room first (Redis + local)
        old_room, _ = await _leave_room(sid)
        if old_room and old_room != room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)

        # Atomic capacity check + add via Lua — prevents TOCTOU race across instances.
        # Returns 1 if added successfully, 0 if room is full.
        admitted = await _redis_atomic_join(sid, room, name)
        if not admitted:
            await sio.emit("error", {"code": "ROOM_FULL", "msg": f"Room is full ({MAX_ROOM_SIZE} max)"}, to=sid)
            log.warning("Room %s full — rejected %s", room, sid)
            return

        await sio.enter_room(sid, room)

        # Members list is now cross-instance via Redis
        members = await _room_members(room)
        await asyncio.gather(
            sio.emit("peer_joined", {"sid": sid, "name": name}, room=room, skip_sid=sid),
            sio.emit("room_state",  {"members": members}, to=sid),
        )
        log.info("[+] %-24s @%-16s joined  %-20s (%d total)", sid, name, room, len(members))

    except Exception as exc:
        log.exception("join_room error sid=%s: %s", sid, exc)


@sio.event
async def leave_room_event(sid: str, data: dict) -> None:
    try:
        old_room, name = await _leave_room(sid)
        if old_room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)
    except Exception as exc:
        log.exception("leave_room_event error sid=%s: %s", sid, exc)


@sio.event
async def update_name(sid: str, data: dict) -> None:
    try:
        new_name = _sanitize_name(data.get("name", ""), "")
        if not new_name:
            return
        info = _local_users.get(sid)
        if not info:
            return
        old_name       = info["name"]
        info["name"]   = new_name
        room           = info.get("room")
        # Update Redis too
        if _redis:
            await _redis.hset(_RK_USER + sid, "name", new_name)
        if room:
            await sio.emit("peer_name_updated", {"sid": sid, "name": new_name}, room=room, skip_sid=sid)
        log.info("   @%-16s renamed -> @%s", old_name, new_name)
    except Exception as exc:
        log.exception("update_name error sid=%s: %s", sid, exc)


@sio.event
async def voice_message(sid: str, data: dict) -> None:
    try:
        # Look up room — Redis is authoritative
        room = await _redis_get_user_room(sid)
        if not room:
            return

        # Distributed rate check
        if not await _check_rate(sid):
            await sio.emit("error", {"code": "RATE_LIMITED", "msg": "Sending too fast"}, to=sid)
            return

        name   = (_local_users.get(sid) or {}).get("name", sid[:6])
        audio  = data.get("audio", "")
        mime   = str(data.get("mime", "audio/webm"))
        msg_id = str(data.get("msg_id", ""))[:64]

        try:
            duration = float(data.get("duration") or 0)
        except (TypeError, ValueError):
            duration = 0.0
        duration = min(duration, MAX_DURATION)

        if not audio:
            return
        audio_len = len(audio)
        if audio_len > MAX_AUDIO_BYTES:
            await sio.emit("error", {"code": "MSG_TOO_LARGE", "msg": "Audio too large"}, to=sid)
            return

        if mime not in ALLOWED_MIME:
            mime = "audio/webm"

        # sio.emit with Redis manager broadcasts cross-instance automatically
        await sio.emit(
            "voice_message",
            {
                "audio":       audio,
                "mime":        mime,
                "duration":    round(duration, 1),
                "msg_id":      msg_id,
                "sender_sid":  sid,
                "sender_name": name,
            },
            room=room,
            skip_sid=sid,
        )
        log.info("   Voice  @%-16s -> %-20s  %.1fs  %d B", name, room, duration, audio_len)

    except Exception as exc:
        log.exception("voice_message error sid=%s: %s", sid, exc)


@sio.event
async def msg_delivered(sid: str, data: dict) -> None:
    try:
        msg_id = str(data.get("msg_id", ""))[:64]
        to     = str(data.get("to", ""))
        if not msg_id or not to:
            return
        # Check presence cross-instance
        if not await _redis_sid_exists(to):
            return
        await sio.emit("msg_delivered", {"msg_id": msg_id}, to=to)
    except Exception as exc:
        log.exception("msg_delivered error sid=%s: %s", sid, exc)
