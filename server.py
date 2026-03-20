"""
WalkieTalk — signaling + voice relay server
FastAPI + python-socketio (ASGI)

Run locally:
    uvicorn server:socket_app --host 0.0.0.0 --port 3000 --reload

Deploy on Render / Railway:
    Start command: uvicorn server:socket_app --host 0.0.0.0 --port $PORT
"""

import logging
import re
import time
from collections import defaultdict

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

# ── Constants (FIX 7: module-level, not recreated per call) ───────────────────
MAX_ROOM_SIZE   = 20
MAX_NAME_LEN    = 32
MAX_ROOM_LEN    = 40
MAX_AUDIO_BYTES = 8_000_000          # 8 MB base64 (~6 MB audio)
MAX_DURATION    = 65.0               # just above client 60s limit
MAX_MSG_RATE    = 4                  # max voice messages per user per 10s window
MSG_RATE_WINDOW = 10.0               # seconds

# FIX 7: module-level constant, not re-created on every call
ALLOWED_MIME = frozenset({
    "audio/webm",
    "audio/webm;codecs=opus",
    "audio/mp4",
    "audio/ogg",
    "audio/wav",
})

# Regex for sanitization (FIX 4+5: compiled once)
_NAME_RE = re.compile(r"[^a-z0-9_\-]")
_ROOM_RE = re.compile(r"[^A-Z0-9_\-]")

# FIX 8: _start_time declared before health endpoint uses it
_start_time = time.time()

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(title="WalkieTalk", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
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

# ── State ─────────────────────────────────────────────────────────────────────
# sid  → { room: str|None, name: str, joined_at: float, msg_times: list[float] }
users: dict[str, dict] = {}

# room → set of sids  (FIX 10: plain dict, not defaultdict, to avoid phantom keys)
rooms: dict[str, set]  = {}

# ── Health endpoint ───────────────────────────────────────────────────────────
@app.get("/health")
async def health(request: Request):
    return JSONResponse({
        "status":      "ok",
        "connections": len(users),
        "rooms":       len(rooms),
        "uptime_s":    round(time.time() - _start_time),
    })

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sanitize_name(raw: str, fallback: str) -> str:
    """Lowercase, strip non-alphanumeric (except _ -), truncate. (FIX 4)"""
    cleaned = _NAME_RE.sub("", (raw or "").strip().lower().replace(" ", "_"))
    return cleaned[:MAX_NAME_LEN] or fallback[:MAX_NAME_LEN]


def _sanitize_room(raw: str) -> str:
    """Uppercase, strip non-alphanumeric (except _ -), truncate. (FIX 5)"""
    return _ROOM_RE.sub("", (raw or "").strip().upper())[:MAX_ROOM_LEN]


def _room_members(room_id: str) -> list[dict]:
    """Return [{sid, name}] for all users in a room. O(room_size)."""
    return [
        {"sid": sid, "name": users[sid]["name"]}
        for sid in rooms.get(room_id, set())
        if sid in users
    ]


def _leave_room(sid: str) -> tuple[str | None, str]:
    """
    Remove sid from its current room index.
    Also clears users[sid]["room"] to None. (FIX 1+3)
    Returns (old_room_id, name).
    """
    info = users.get(sid)
    if not info:
        return None, sid[:6]

    old_room = info.get("room")
    name     = info.get("name", sid[:6])

    if old_room:
        room_set = rooms.get(old_room)
        if room_set is not None:
            room_set.discard(sid)
            # FIX 10: clean up only if empty; plain dict avoids phantom re-creation
            if not room_set:
                del rooms[old_room]
        # FIX 1+3: always sync users[sid]["room"] to None
        info["room"] = None

    return old_room, name


def _check_rate(sid: str) -> bool:
    """
    FIX 11: simple sliding-window rate limiter for voice messages.
    Returns True if the message is allowed, False if rate exceeded.
    """
    info = users.get(sid)
    if not info:
        return False
    now   = time.monotonic()
    times = info.setdefault("msg_times", [])
    # Evict old timestamps outside the window
    cutoff = now - MSG_RATE_WINDOW
    info["msg_times"] = [t for t in times if t > cutoff]
    if len(info["msg_times"]) >= MAX_MSG_RATE:
        return False
    info["msg_times"].append(now)
    return True


# ── Socket events ─────────────────────────────────────────────────────────────

@sio.event
async def connect(sid: str, environ: dict):
    # FIX 12: don't log users+1; user isn't registered yet
    log.info("[+] connected   %s", sid)


@sio.event
async def disconnect(sid: str):
    # FIX 2: use try/finally so users.pop always runs even if _leave_room raises
    old_room = name = None
    try:
        old_room, name = _leave_room(sid)
    except Exception as exc:
        log.exception("_leave_room error on disconnect for %s: %s", sid, exc)
    finally:
        users.pop(sid, None)

    log.info("[-] disconnect  %-24s @%s  (room=%s)", sid, name, old_room)
    if old_room:
        await sio.emit(
            "peer_left", {"sid": sid, "name": name},
            room=old_room, skip_sid=sid,
        )


@sio.event
async def join_room(sid: str, data: dict):
    try:
        room = _sanitize_room(data.get("room", ""))
        name = _sanitize_name(data.get("name", ""), sid[:6])

        if not room:
            return

        # FIX 6: leave current room first, THEN check new room size
        # (so rejoining your own room never gets blocked by your own seat)
        current_room = users.get(sid, {}).get("room")
        old_room, _  = _leave_room(sid)

        if old_room and old_room != room:
            await sio.leave_room(sid, old_room)
            await sio.emit(
                "peer_left", {"sid": sid, "name": name},
                room=old_room, skip_sid=sid,
            )
            log.info("   %s left %s", sid, old_room)

        # Now check room capacity (after leaving, so rejoin always succeeds)
        new_size = len(rooms.get(room, set()))
        if new_size >= MAX_ROOM_SIZE:
            await sio.emit(
                "error",
                {"code": "ROOM_FULL", "msg": f"Room is full ({MAX_ROOM_SIZE} max)"},
                to=sid,
            )
            log.warning("   Room %s full (%d), rejected %s", room, new_size, sid)
            return

        # Register user (preserve msg_times for rate limiting continuity)
        existing = users.get(sid, {})
        users[sid] = {
            "room":      room,
            "name":      name,
            "joined_at": time.time(),
            "msg_times": existing.get("msg_times", []),
        }
        rooms.setdefault(room, set()).add(sid)
        await sio.enter_room(sid, room)

        # Notify others first, then send room state to joiner
        await sio.emit(
            "peer_joined", {"sid": sid, "name": name},
            room=room, skip_sid=sid,
        )
        members = _room_members(room)
        await sio.emit("room_state", {"members": members}, to=sid)

        log.info("   @%-16s joined  %-20s (%d members)", name, room, len(members))

    except Exception as exc:
        log.exception("join_room error for %s: %s", sid, exc)


@sio.event
async def leave_room_event(sid: str, data: dict):
    try:
        old_room, name = _leave_room(sid)   # also sets users[sid]["room"] = None
        if old_room:
            await sio.leave_room(sid, old_room)
            await sio.emit(
                "peer_left", {"sid": sid, "name": name},
                room=old_room, skip_sid=sid,
            )
            log.info("   @%-16s left    %s", name, old_room)
    except Exception as exc:
        log.exception("leave_room_event error for %s: %s", sid, exc)


@sio.event
async def update_name(sid: str, data: dict):
    try:
        new_name = _sanitize_name(data.get("name", ""), "")
        if not new_name or sid not in users:
            return
        old_name = users[sid]["name"]
        users[sid]["name"] = new_name
        room = users[sid].get("room")
        if room:
            await sio.emit(
                "peer_name_updated", {"sid": sid, "name": new_name},
                room=room, skip_sid=sid,
            )
        log.info("   @%-16s renamed → @%s", old_name, new_name)
    except Exception as exc:
        log.exception("update_name error for %s: %s", sid, exc)


@sio.event
async def voice_message(sid: str, data: dict):
    try:
        info = users.get(sid)
        if not info:
            return
        room = info.get("room")
        if not room:
            return

        # FIX 11: rate limit
        if not _check_rate(sid):
            await sio.emit(
                "error",
                {"code": "RATE_LIMITED", "msg": "Sending too fast — wait a moment"},
                to=sid,
            )
            log.warning("   Rate-limited @%s in %s", info["name"], room)
            return

        audio    = data.get("audio", "")
        mime     = str(data.get("mime", "audio/webm"))
        msg_id   = str(data.get("msg_id", ""))[:64]

        # FIX 9: validate duration cleanly without raising ValueError
        try:
            duration = float(data.get("duration") or 0)
        except (TypeError, ValueError):
            duration = 0.0
        duration = min(duration, MAX_DURATION)

        if not audio:
            return
        if len(audio) > MAX_AUDIO_BYTES:
            await sio.emit(
                "error",
                {"code": "MSG_TOO_LARGE", "msg": "Audio too large"},
                to=sid,
            )
            log.warning("   Too large from @%s  (%d B)", info["name"], len(audio))
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
            "   Voice  @%-16s → %-20s  %.1fs  %d B",
            info["name"], room, duration, len(audio),
        )

    except Exception as exc:
        log.exception("voice_message error for %s: %s", sid, exc)


@sio.event
async def msg_delivered(sid: str, data: dict):
    try:
        msg_id = str(data.get("msg_id", ""))[:64]
        to     = str(data.get("to", ""))
        if not msg_id or not to or to not in users:
            return
        await sio.emit("msg_delivered", {"msg_id": msg_id}, to=to)
    except Exception as exc:
        log.exception("msg_delivered error for %s: %s", sid, exc)
