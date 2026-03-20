import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins='*',
    ping_timeout=60,
    ping_interval=25,
    max_http_buffer_size=10 * 1024 * 1024,  # 10 MB for audio blobs
)

socket_app = socketio.ASGIApp(sio, app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# sid -> room_id
user_rooms = {}


@sio.event
async def connect(sid, environ):
    print(f"[+] Connected:    {sid}")


@sio.event
async def disconnect(sid):
    print(f"[-] Disconnected: {sid}")
    room_id = user_rooms.get(sid)
    if room_id:
        await sio.emit('peer-left', sid, room=room_id, skip_sid=sid)
        await sio.leave_room(sid, room_id)
        del user_rooms[sid]
        print(f"    Left room: {room_id}")


@sio.event
async def join_room(sid, room_id):
    # Leave previous room if switching
    old_room = user_rooms.get(sid)
    if old_room and old_room != room_id:
        await sio.emit('peer-left', sid, room=old_room, skip_sid=sid)
        await sio.leave_room(sid, old_room)
        print(f"    {sid} left old room: {old_room}")

    await sio.enter_room(sid, room_id)
    user_rooms[sid] = room_id
    await sio.emit('peer-joined', sid, room=room_id, skip_sid=sid)
    print(f"    {sid} joined room: {room_id}")


@sio.event
async def voice_message(sid, data):
    """
    Relay recorded voice message to target peer.
    data = { to, audio (base64), mime, duration }
    """
    to       = data.get('to')
    audio    = data.get('audio')
    mime     = data.get('mime', 'audio/webm')
    duration = data.get('duration', 0)

    if not to or not audio:
        print(f"    [!] voice_message missing fields from {sid}")
        return

    await sio.emit('voice_message', {
        'audio':    audio,
        'mime':     mime,
        'duration': duration,
    }, to=to)
    print(f"    Voice msg: {sid} -> {to} ({duration:.1f}s, {mime})")
