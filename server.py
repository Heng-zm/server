"""
WalkieTalk — signaling + voice relay server
FastAPI + python-socketio (ASGI)

Environment variables (set in Render / Railway / .env):
    SUPABASE_URL   = https://bgqeqiyfgpdvgeepignt.supabase.co
    SUPABASE_KEY   = sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6

Run locally:
    SUPABASE_URL=... SUPABASE_KEY=... uvicorn server:socket_app --host 0.0.0.0 --port 3000 --reload

Deploy on Render / Railway:
    Start command: uvicorn server:socket_app --host 0.0.0.0 --port $PORT
"""

import asyncio
import logging
import os
import re
import time
from collections import deque
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

# ── Supabase config — read from environment, never hardcoded ─────────────────
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://bgqeqiyfgpdvgeepignt.supabase.co",   # fallback for local dev only
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "sb_publishable_eLoAp9t0x-t7id3a-3LUow_SaBM6EC6",
)
_SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
}

# ── Signal/Relay constants ────────────────────────────────────────────────────
MAX_ROOM_SIZE   = 20
MAX_NAME_LEN    = 32
MAX_ROOM_LEN    = 40
MAX_AUDIO_BYTES = 8_000_000
MAX_DURATION    = 65.0
MAX_MSG_RATE    = 4
MSG_RATE_WINDOW = 10.0

ALLOWED_MIME: frozenset[str] = frozenset({
    "audio/webm",
    "audio/webm;codecs=opus",
    "audio/mp4",
    "audio/ogg",
    "audio/wav",
})

_NAME_RE    = re.compile(r"[^a-z0-9_\-]")
_ROOM_RE    = re.compile(r"[^A-Z0-9_\-]")
_DEVICE_RE  = re.compile(r"[^a-zA-Z0-9_\-]")   # device_id sanitizer
_start_time = time.time()

# ── Shared async HTTP client for Supabase calls ───────────────────────────────
_http: httpx.AsyncClient | None = None


# ── Lifespan (replaces deprecated @app.on_event) ─────────────────────────────
@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _http
    _http = httpx.AsyncClient(
        base_url=SUPABASE_URL,
        headers=_SB_HEADERS,
        timeout=10.0,
    )
    log.info(
        "WalkieTalk started  pid=%d  supabase=%s",
        os.getpid(), SUPABASE_URL,
    )
    yield
    await _http.aclose()
    log.info("WalkieTalk stopping  connections=%d", len(users))


# ── App & socket ──────────────────────────────────────────────────────────────
app = FastAPI(title="WalkieTalk", docs_url=None, redoc_url=None, lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
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

# ── In-memory socket state ────────────────────────────────────────────────────
users: dict[str, dict] = {}
rooms: dict[str, set]  = {}


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({
        "status":      "ok",
        "connections": len(users),
        "rooms":       {k: len(v) for k, v in rooms.items()},
        "uptime_s":    round(time.time() - _start_time),
        "supabase_url": SUPABASE_URL,
        "http_ready":   _http is not None,
    })


@app.get("/zones/ping")
async def zones_ping() -> JSONResponse:
    """Diagnostic: verify Supabase table exists and key is valid."""
    if _http is None:
        return JSONResponse({"ok": False, "error": "http client not ready"}, status_code=503)
    try:
        r = await _http.get(
            "/rest/v1/geo_zones",
            params={"limit": "1", "select": "id"},
        )
        return JSONResponse({
            "ok":     r.is_success,
            "status": r.status_code,
            "body":   r.text[:500],
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── Supabase proxy: GET /zones?device_id=<id> ────────────────────────────────
@app.get("/zones")
async def get_zones(request: Request) -> JSONResponse:
    """
    Return ALL zones from Supabase — visible to every device.
    device_id is included so the client knows which zones it owns.
    """
    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    try:
        r = await _http.get(
            "/rest/v1/geo_zones",
            params={
                "order":  "created_at.asc",
                "select": "id,device_id,name,channel,lat,lng,radius,color,auto_join,created_by",
            },
        )
        if not r.is_success:
            log.error("Supabase GET failed status=%s body=%s", r.status_code, r.text)
            return JSONResponse(
                {"error": "upstream error", "status": r.status_code, "detail": r.text},
                status_code=502,
            )
        return JSONResponse(r.json())
    except Exception as e:
        log.exception("get_zones error: %s", e)
        return JSONResponse({"error": "server error", "detail": str(e)}, status_code=500)


# ── Supabase proxy: POST /zones ───────────────────────────────────────────────
@app.post("/zones")
async def upsert_zone(request: Request) -> JSONResponse:
    """
    Proxy upsert to Supabase geo_zones.
    Validates and sanitizes all fields server-side.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    device_id = _sanitize_device(body.get("device_id", ""))
    zone_id   = _sanitize_device(body.get("id", ""))
    name      = str(body.get("name", ""))[:40].strip()
    channel   = _sanitize_room(str(body.get("channel", "")))
    color     = _validate_color(body.get("color", "#007aff"))
    auto_join = bool(body.get("auto_join", True))

    try:
        lat    = float(body["lat"])
        lng    = float(body["lng"])
        radius = int(body["radius"])
    except (KeyError, TypeError, ValueError):
        return JSONResponse({"error": "lat/lng/radius required and must be numeric"}, status_code=400)

    if not device_id or not zone_id or not channel:
        return JSONResponse({"error": "device_id, id, channel required"}, status_code=400)
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        return JSONResponse({"error": "invalid coordinates"}, status_code=400)
    if not (10 <= radius <= 50_000):
        return JSONResponse({"error": "radius must be 10–50000 m"}, status_code=400)

    created_by = _sanitize_name(str(body.get("created_by", "")), "")

    payload = {
        "id":         zone_id,
        "device_id":  device_id,
        "name":       name or channel,
        "channel":    channel,
        "lat":        lat,
        "lng":        lng,
        "radius":     radius,
        "color":      color,
        "auto_join":  auto_join,
        "created_by": created_by[:32],
    }

    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    log.info("upsert_zone payload: %s", payload)
    try:
        r = await _http.post(
            "/rest/v1/geo_zones",
            json=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
        if not r.is_success:
            log.error("Supabase upsert failed status=%s body=%s", r.status_code, r.text)
            return JSONResponse(
                {"error": "upstream error", "status": r.status_code, "detail": r.text},
                status_code=502,
            )
        # Broadcast to all connected socket clients so every device updates instantly
        await sio.emit("zone_upserted", {
            "id":         zone_id,
            "device_id":  device_id,
            "name":       payload["name"],
            "channel":    channel,
            "lat":        lat,
            "lng":        lng,
            "radius":     radius,
            "color":      color,
            "auto_join":  auto_join,
            "created_by": payload.get("created_by", ""),
        })
        log.info("zone_upserted broadcast id=%s by device=%s", zone_id, device_id[:12])
        return JSONResponse({"ok": True})
    except Exception as e:
        log.exception("upsert_zone error: %s", e)
        return JSONResponse({"error": "server error", "detail": str(e)}, status_code=500)


# ── Supabase proxy: DELETE /zones/<id>?device_id=<device_id> ─────────────────
@app.delete("/zones/{zone_id}")
async def delete_zone(zone_id: str, request: Request) -> JSONResponse:
    """
    Proxy DELETE to Supabase. Enforces device_id ownership server-side.
    """
    device_id = _sanitize_device(request.query_params.get("device_id", ""))
    zone_id   = _sanitize_device(zone_id)
    if not device_id or not zone_id:
        return JSONResponse({"error": "device_id and zone_id required"}, status_code=400)

    if _http is None:
        return JSONResponse({"error": "server initializing"}, status_code=503)
    try:
        r = await _http.delete(
            "/rest/v1/geo_zones",
            params={
                "id":        f"eq.{zone_id}",
                "device_id": f"eq.{device_id}",
            },
        )
        if not r.is_success:
            log.error("Supabase DELETE failed status=%s body=%s", r.status_code, r.text)
            return JSONResponse(
                {"error": "upstream error", "status": r.status_code, "detail": r.text},
                status_code=502,
            )
        # Broadcast deletion to all connected clients
        await sio.emit("zone_deleted", {"id": zone_id, "device_id": device_id})
        log.info("zone_deleted broadcast id=%s by device=%s", zone_id, device_id[:12])
        return JSONResponse({"ok": True})
    except Exception as e:
        log.exception("delete_zone error: %s", e)
        return JSONResponse({"error": "server error", "detail": str(e)}, status_code=500)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _sanitize_name(raw: str, fallback: str) -> str:
    cleaned = _NAME_RE.sub("", (raw or "").strip().lower().replace(" ", "_"))
    return cleaned[:MAX_NAME_LEN] or fallback[:MAX_NAME_LEN]


def _sanitize_room(raw: str) -> str:
    return _ROOM_RE.sub("", (raw or "").strip().upper())[:MAX_ROOM_LEN]


def _sanitize_device(raw: str) -> str:
    """Strip anything that isn't alphanumeric, _, or - from device/zone IDs."""
    return _DEVICE_RE.sub("", (raw or "").strip())[:128]


def _validate_color(raw: object) -> str:
    """Accept hex colors only. Falls back to blue."""
    s = str(raw or "").strip()
    import re as _re
    return s if _re.match(r"^#[0-9a-fA-F]{6}$", s) else "#007aff"


def _room_members(room_id: str) -> list[dict]:
    snapshot = frozenset(rooms.get(room_id, set()))
    return [
        {"sid": sid, "name": users[sid]["name"]}
        for sid in snapshot
        if sid in users
    ]


def _leave_room(sid: str) -> tuple[str | None, str]:
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
    info = users.get(sid)
    if not info:
        return False
    now    = time.monotonic()
    times: deque = info.setdefault("msg_times", deque())
    cutoff = now - MSG_RATE_WINDOW
    while times and times[0] <= cutoff:
        times.popleft()
    if len(times) >= MAX_MSG_RATE:
        return False
    times.append(now)
    return True


# ── Socket events ─────────────────────────────────────────────────────────────

@sio.event
async def connect(sid: str, environ: dict) -> None:
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
        users.pop(sid, None)

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

        old_room, _ = _leave_room(sid)
        if old_room and old_room != room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)

        new_size = len(rooms.get(room, set()))
        if new_size >= MAX_ROOM_SIZE:
            await sio.emit("error", {"code": "ROOM_FULL", "msg": f"Room is full ({MAX_ROOM_SIZE} max)"}, to=sid)
            return

        existing = users.get(sid, {})
        users[sid] = {
            "room":      room,
            "name":      name,
            "joined_at": time.time(),
            "msg_times": existing.get("msg_times", deque()),
        }
        rooms.setdefault(room, set()).add(sid)
        await sio.enter_room(sid, room)

        members = _room_members(room)
        await asyncio.gather(
            sio.emit("peer_joined", {"sid": sid, "name": name}, room=room, skip_sid=sid),
            sio.emit("room_state",  {"members": members},        to=sid),
        )
        log.info("[+] %-24s @%-16s joined  %-20s (%d)", sid, name, room, len(members))

    except Exception as exc:
        log.exception("join_room error sid=%s: %s", sid, exc)


@sio.event
async def leave_room_event(sid: str, data: dict) -> None:
    try:
        old_room, name = _leave_room(sid)
        if old_room:
            await sio.leave_room(sid, old_room)
            await sio.emit("peer_left", {"sid": sid, "name": name}, room=old_room, skip_sid=sid)
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
            await sio.emit("peer_name_updated", {"sid": sid, "name": new_name}, room=room, skip_sid=sid)
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
            await sio.emit("error", {"code": "RATE_LIMITED", "msg": "Sending too fast"}, to=sid)
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

        audio_len = len(audio)
        if audio_len > MAX_AUDIO_BYTES:
            await sio.emit("error", {"code": "MSG_TOO_LARGE", "msg": "Audio too large"}, to=sid)
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
        log.info("   Voice  @%-16s -> %-20s  %.1fs  %d B", info["name"], room, duration, audio_len)

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
