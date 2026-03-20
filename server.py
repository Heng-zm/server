"""
WalkieTalk signaling + voice relay server
FastAPI + python-socketio (ASGI)

Run:
    uvicorn server:socket_app --host 0.0.0.0 --port 3000 --reload
"""

import logging
import time
from collections import defaultdict

import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("walkie")

# ── Limits ───────────────────────────────────────────────────────────────────
MAX_ROOM_SIZE   = 20          # max users per room
MAX_NAME_LEN    = 32          # characters
MAX_ROOM_LEN    = 40          # characters
MAX_AUDIO_BYTES = 8_000_000   # 8 MB base64 string (~6 MB audio)
MAX_DURATION    = 65.0        # seconds (slightly above client MAX_REC_SEC=60)

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(title="WalkieTalk", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    # FIX 9: shorter timeouts → faster detection of dropped mobile connections
    ping_timeout=20,
    ping_interval=15,
    max_http_buffer_size=MAX_AUDIO_BYTES + 512_000,  # audio + metadata headroom
    logger=False,
    engineio_logger=False,
)

socket_app = socketio.ASGIApp(sio, app)

# ── State ─────────────────────────────────────────────────────────────────────
# FIX 1+2: dual index for O(1) lookups in both directions
# sid   → { room, name, joined_at }
# room  → set of sids
users: dict[str, dict] = {}
rooms: dict[str, set]  = defaultdict(set)   # room_id → {sid, ...}

# ── Health endpoint (FIX 10) ──────────────────────────────────────────────────
@app.get("/health")
async def health():
    return JSONResponse({
        "status":       "ok",
        "connections":  len(users),
        "rooms":        len(rooms),
        "uptime_s":     round(time.time() - _start_time),
    })

_start_time = time.time()

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sanitize_name(raw: str, fallback: str) -> str:
    """Lowercase, strip, truncate, replace spaces."""
    return (raw or "").strip().lower().replace(" ", "_")[:MAX_NAME_LEN] or fallback[:MAX_NAME_LEN]

def _sanitize_room(raw: str) -> str:
    """Uppercase, strip, truncate."""
    return (raw or "").strip().upper()[:MAX_ROOM_LEN]

def _room_members(room_id: str) -> list[dict]:
    """O(room_size) — uses rooms index, not full users scan (FIX 1)."""
    return [
        {"sid": sid, "name": users[sid]["name"]}
        for sid in rooms.get(room_id, set())
        if sid in users
    ]

def _leave_room(sid: str) -> tuple[str | None, str]:
    """Remove sid from its current room. Returns (room_id, name)."""
    info = users.get(sid)
    if not info:
        return None, sid[:6]
    old_room = info.get("room")
    name     = info.get("name", sid[:6])
    if old_room:
        rooms[old_room].discard(sid)
        if not rooms[old_room]:        # clean up empty rooms
            del rooms[old_room]
    return old_room, name

# ── Socket events ─────────────────────────────────────────────────────────────

@sio.event
async def connect(sid: str, environ: dict):
    log.info("[+] %-24s connected  (total=%d)", sid, len(users) + 1)


@sio.event
async def disconnect(sid: str):
    # FIX 11: don't call sio.leave_room — socket-io already cleaned up rooms
    old_room, name = _leave_room(sid)
    users.pop(sid, None)
    log.info("[-] %-24s @%-16s disconnected from %s", sid, name, old_room)
    if old_room:
        await sio.emit("peer_left", {"sid": sid, "name": name},
                       room=old_room, skip_sid=sid)


@sio.event
async def join_room(sid: str, data: dict):
    # FIX 12: wrap in try/except
    try:
        room = _sanitize_room(data.get("room", ""))
        name = _sanitize_name(data.get("name", ""), sid[:6])

        if not room:
            return

        # FIX 8: enforce room size limit
        current_room_size = len(rooms.get(room, set()))
        if current_room_size >= MAX_ROOM_SIZE:
            await sio.emit("error", {"code": "ROOM_FULL", "msg": f"Room is full ({MAX_ROOM_SIZE} max)"}, to=sid)
            log.warning("   Room %s full (%d), rejected %s", room, current_room_size, sid)
            return

        # Leave old room if switching
        old_room, _ = _leave_room(sid)
        if old_room and old_room != room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name},
                           room=old_room, skip_sid=sid)
            log.info("   %s left %s", sid, old_room)

        # Register user
        users[sid] = {"room": room, "name": name, "joined_at": time.time()}
        rooms[room].add(sid)
        await sio.enter_room(sid, room)

        # Notify others
        await sio.emit("peer_joined", {"sid": sid, "name": name},
                       room=room, skip_sid=sid)

        # Send room state to joiner
        members = _room_members(room)
        await sio.emit("room_state", {"members": members}, to=sid)

        log.info("   @%-16s joined  %-20s (%d members)", name, room, len(members))

    except Exception as exc:
        log.exception("join_room error for %s: %s", sid, exc)


@sio.event
async def leave_room_event(sid: str, data: dict):
    # FIX 4: leave_room no longer removes user from users dict entirely
    try:
        old_room, name = _leave_room(sid)
        if old_room:
            users[sid]["room"] = None
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name},
                           room=old_room, skip_sid=sid)
            log.info("   @%-16s left    %s", name, old_room)
    except Exception as exc:
        log.exception("leave_room error for %s: %s", sid, exc)


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
            await sio.emit("peer_name_updated", {"sid": sid, "name": new_name},
                           room=room, skip_sid=sid)
        log.info("   @%-16s renamed → @%s", old_name, new_name)
    except Exception as exc:
        log.exception("update_name error for %s: %s", sid, exc)


@sio.event
async def voice_message(sid: str, data: dict):
    try:
        # FIX 7: look up room from server state, ignore client-supplied room
        info = users.get(sid)
        if not info:
            return
        room = info.get("room")
        if not room:
            log.warning("   voice_message from %s who is not in a room", sid)
            return

        audio       = data.get("audio", "")
        mime        = data.get("mime", "audio/webm")
        duration    = float(data.get("duration") or 0)
        msg_id      = str(data.get("msg_id", ""))[:64]     # FIX 6: cap length
        sender_name = info["name"]

        # FIX 6: validate payload sizes
        if not audio:
            return
        if len(audio) > MAX_AUDIO_BYTES:
            log.warning("   voice_message too large from @%s (%d bytes)", sender_name, len(audio))
            await sio.emit("error", {"code": "MSG_TOO_LARGE", "msg": "Audio too large"}, to=sid)
            return
        if duration > MAX_DURATION:
            duration = MAX_DURATION

        # Validate mime to a safe allowlist
        ALLOWED_MIME = {"audio/webm", "audio/webm;codecs=opus", "audio/mp4", "audio/ogg", "audio/wav"}
        if mime not in ALLOWED_MIME:
            mime = "audio/webm"

        await sio.emit("voice_message", {
            "audio":       audio,
            "mime":        mime,
            "duration":    round(duration, 1),
            "msg_id":      msg_id,
            "sender_sid":  sid,
            "sender_name": sender_name,
        }, room=room, skip_sid=sid)

        log.info("   Voice  @%-16s → %-20s  %.1fs  %d B", sender_name, room, duration, len(audio))

    except Exception as exc:
        log.exception("voice_message error for %s: %s", sid, exc)


@sio.event
async def msg_delivered(sid: str, data: dict):
    try:
        msg_id = str(data.get("msg_id", ""))[:64]
        to     = str(data.get("to", ""))

        # FIX 7: only relay receipt if 'to' is a real connected user
        if not msg_id or not to or to not in users:
            return

        await sio.emit("msg_delivered", {"msg_id": msg_id}, to=to)

    except Exception as exc:
        log.exception("msg_delivered error for %s: %s", sid, exc)
