"""
WalkieTalk — signaling + voice relay server
FastAPI + python-socketio (ASGI)

Run locally:
    uvicorn server:socket_app --host 0.0.0.0 --port 3000 --reload

Deploy on Render / Railway:
    Start command: uvicorn server:socket_app --host 0.0.0.0 --port $PORT
"""

import asyncio
import logging
import re
import time
from collections import deque

import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("walkie")

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_ROOM_SIZE   = 20
MAX_NAME_LEN    = 32
MAX_ROOM_LEN    = 40
MAX_AUDIO_BYTES = 8_000_000      # 8 MB base64 (~6 MB audio)
MAX_DURATION    = 65.0           # seconds — just above client 60s limit
MAX_MSG_RATE    = 4              # voice messages allowed per window
MSG_RATE_WINDOW = 10.0           # sliding window in seconds

ALLOWED_MIME: frozenset[str] = frozenset({
    "audio/webm",
    "audio/webm;codecs=opus",
    "audio/mp4",
    "audio/ogg",
    "audio/wav",
})

# Compiled once at import — never recreated per-call
_NAME_RE = re.compile(r"[^a-z0-9_\-]")
_ROOM_RE = re.compile(r"[^A-Z0-9_\-]")

_start_time = time.time()

# ── App & socket setup ────────────────────────────────────────────────────────
app = FastAPI(title="WalkieTalk", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],       # Socket.IO transports need full method set
    allow_headers=["*"],
)

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    ping_timeout=20,
    ping_interval=15,
    max_http_buffer_size=MAX_AUDIO_BYTES + 512_000,
    logger=False,
    engineio_logger=False,
)

socket_app = socketio.ASGIApp(sio, app)

# ── In-memory state ───────────────────────────────────────────────────────────
# sid -> { room: str|None, name: str, joined_at: float, msg_times: deque[float] }
users: dict[str, dict] = {}

# room_id -> set of sids  (plain dict avoids phantom keys from defaultdict)
rooms: dict[str, set]  = {}

# ── Lifecycle ─────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def _startup() -> None:
    log.info("WalkieTalk server started  (pid=%d)", __import__("os").getpid())

@app.on_event("shutdown")
async def _shutdown() -> None:
    log.info("WalkieTalk server stopping  (connections=%d)", len(users))

# ── Health endpoint ───────────────────────────────────────────────────────────
@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({
        "status":      "ok",
        "connections": len(users),
        "rooms":       {k: len(v) for k, v in rooms.items()},
        "uptime_s":    round(time.time() - _start_time),
    })

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sanitize_name(raw: str, fallback: str) -> str:
    """Lowercase, spaces -> underscores, strip non [a-z0-9_-], truncate."""
    cleaned = _NAME_RE.sub("", (raw or "").strip().lower().replace(" ", "_"))
    return cleaned[:MAX_NAME_LEN] or fallback[:MAX_NAME_LEN]


def _sanitize_room(raw: str) -> str:
    """Uppercase, strip non [A-Z0-9_-], truncate."""
    return _ROOM_RE.sub("", (raw or "").strip().upper())[:MAX_ROOM_LEN]


def _room_members(room_id: str) -> list[dict]:
    """
    Snapshot the room's sid set before iterating so a concurrent coroutine
    cannot cause a RuntimeError on set mutation mid-loop.
    """
    snapshot = frozenset(rooms.get(room_id, set()))
    return [
        {"sid": sid, "name": users[sid]["name"]}
        for sid in snapshot
        if sid in users
    ]


def _leave_room(sid: str) -> tuple[str | None, str]:
    """
    Remove sid from its room index and clear users[sid]["room"].
    Returns (old_room_id, display_name). Safe to call if sid is unknown.
    """
    info = users.get(sid)
    if not info:
        return None, sid[:6]

    old_room = info.get("room")
    name     = info.get("name") or sid[:6]

    if old_room:
        room_set = rooms.get(old_room)
        if room_set is not None:
            room_set.discard(sid)
            if not room_set:
                del rooms[old_room]
        info["room"] = None

    return old_room, name


def _check_rate(sid: str) -> bool:
    """
    Sliding-window rate limiter using a deque for O(1) amortised eviction.
    Returns True when the message should be allowed through.
    """
    info = users.get(sid)
    if not info:
        return False

    now    = time.monotonic()
    times: deque = info.setdefault("msg_times", deque())
    cutoff = now - MSG_RATE_WINDOW

    # Pop expired timestamps from the left — O(1) per pop on a deque
    while times and times[0] <= cutoff:
        times.popleft()

    if len(times) >= MAX_MSG_RATE:
        return False

    times.append(now)
    return True


# ── Socket events ─────────────────────────────────────────────────────────────

@sio.event
async def connect(sid: str, environ: dict) -> None:
    # No user record exists yet — meaningful logging happens in join_room.
    pass


@sio.event
async def disconnect(sid: str) -> None:
    old_room: str | None = None
    name: str | None     = None

    try:
        old_room, name = _leave_room(sid)
    except Exception as exc:
        log.exception("_leave_room error on disconnect sid=%s: %s", sid, exc)
    finally:
        # Always remove the user record, even if _leave_room raised
        users.pop(sid, None)

    log.info("[-] %-24s @%-16s left %s", sid, name or "?", old_room or "(no room)")

    if old_room and name:
        await sio.emit(
            "peer_left",
            {"sid": sid, "name": name},
            room=old_room,
            skip_sid=sid,
        )


@sio.event
async def join_room(sid: str, data: dict) -> None:
    try:
        room = _sanitize_room(data.get("room", ""))
        name = _sanitize_name(data.get("name", ""), sid[:6])

        if not room:
            return

        # Leave current room first so the capacity check below never counts
        # the user against their own seat when they rejoin the same room.
        old_room, _ = _leave_room(sid)

        if old_room and old_room != room:
            await sio.leave_room(sid, old_room)
            await sio.emit(
                "peer_left",
                {"sid": sid, "name": name},
                room=old_room,
                skip_sid=sid,
            )
            log.info("   %-24s left  %s", sid, old_room)

        # Capacity check (after leaving — self-rejoin always succeeds)
        new_size = len(rooms.get(room, set()))
        if new_size >= MAX_ROOM_SIZE:
            await sio.emit(
                "error",
                {"code": "ROOM_FULL", "msg": f"Room is full ({MAX_ROOM_SIZE} max)"},
                to=sid,
            )
            log.warning("   Room %s full (%d) — rejected %s", room, new_size, sid)
            return

        # Register user, preserving rate-limit history across room switches
        existing = users.get(sid, {})
        users[sid] = {
            "room":      room,
            "name":      name,
            "joined_at": time.time(),
            "msg_times": existing.get("msg_times", deque()),
        }
        rooms.setdefault(room, set()).add(sid)
        await sio.enter_room(sid, room)

        # Notify peers and send room state concurrently — saves one round-trip
        members = _room_members(room)
        await asyncio.gather(
            sio.emit("peer_joined", {"sid": sid, "name": name}, room=room, skip_sid=sid),
            sio.emit("room_state",  {"members": members},        to=sid),
        )

        log.info(
            "[+] %-24s @%-16s joined  %-20s (%d members)",
            sid, name, room, len(members),
        )

    except Exception as exc:
        log.exception("join_room error sid=%s: %s", sid, exc)


@sio.event
async def leave_room_event(sid: str, data: dict) -> None:
    try:
        old_room, name = _leave_room(sid)
        if old_room:
            await sio.leave_room(sid, old_room)
            await sio.emit(
                "peer_left",
                {"sid": sid, "name": name},
                room=old_room,
                skip_sid=sid,
            )
            log.info("   %-24s @%-16s voluntarily left %s", sid, name, old_room)
    except Exception as exc:
        log.exception("leave_room_event error sid=%s: %s", sid, exc)


@sio.event
async def update_name(sid: str, data: dict) -> None:
    try:
        new_name = _sanitize_name(data.get("name", ""), "")
        if not new_name or sid not in users:
            return
        old_name           = users[sid]["name"]
        users[sid]["name"] = new_name
        room               = users[sid].get("room")
        if room:
            await sio.emit(
                "peer_name_updated",
                {"sid": sid, "name": new_name},
                room=room,
                skip_sid=sid,
            )
        log.info("   @%-16s renamed -> @%s", old_name, new_name)
    except Exception as exc:
        log.exception("update_name error sid=%s: %s", sid, exc)


@sio.event
async def voice_message(sid: str, data: dict) -> None:
    try:
        info = users.get(sid)
        if not info:
            return

        room = info.get("room")
        if not room:
            return

        if not _check_rate(sid):
            await sio.emit(
                "error",
                {"code": "RATE_LIMITED", "msg": "Sending too fast — wait a moment"},
                to=sid,
            )
            log.warning("   Rate-limited @%s in %s", info["name"], room)
            return

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

        # Measure once — len() on a large str is O(n) in CPython
        audio_len = len(audio)
        if audio_len > MAX_AUDIO_BYTES:
            await sio.emit(
                "error",
                {"code": "MSG_TOO_LARGE", "msg": "Audio too large"},
                to=sid,
            )
            log.warning("   Too large from @%s  (%d B)", info["name"], audio_len)
            return

        if mime not in ALLOWED_MIME:
            mime = "audio/webm"

        await sio.emit(
            "voice_message",
            {
                "audio":       audio,
                "mime":        mime,
                "duration":    round(duration, 1),
                "msg_id":      msg_id,
                "sender_sid":  sid,
                "sender_name": info["name"],
            },
            room=room,
            skip_sid=sid,
        )

        log.info(
            "   Voice  @%-16s -> %-20s  %.1fs  %d B",
            info["name"], room, duration, audio_len,
        )

    except Exception as exc:
        log.exception("voice_message error sid=%s: %s", sid, exc)


@sio.event
async def msg_delivered(sid: str, data: dict) -> None:
    try:
        msg_id = str(data.get("msg_id", ""))[:64]
        to     = str(data.get("to", ""))
        if not msg_id or not to or to not in users:
            return
        await sio.emit("msg_delivered", {"msg_id": msg_id}, to=to)
    except Exception as exc:
        log.exception("msg_delivered error sid=%s: %s", sid, exc)
