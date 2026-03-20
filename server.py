import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins='*',
    ping_timeout=60,
    ping_interval=25,
)

socket_app = socketio.ASGIApp(sio, app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Track which room each user is in
user_rooms = {}


@sio.event
async def connect(sid, environ):
    print(f"[+] Connected:    {sid}")


@sio.event
async def disconnect(sid):
    print(f"[-] Disconnected: {sid}")

    # Notify the peer in the same room that this user left
    room_id = user_rooms.get(sid)
    if room_id:
        await sio.emit('peer-left', sid, room=room_id, skip_sid=sid)
        await sio.leave_room(sid, room_id)
        del user_rooms[sid]
        print(f"    Left room: {room_id}")


@sio.event
async def join_room(sid, room_id):
    # Leave any previous room first
    old_room = user_rooms.get(sid)
    if old_room and old_room != room_id:
        await sio.emit('peer-left', sid, room=old_room, skip_sid=sid)
        await sio.leave_room(sid, old_room)
        print(f"    {sid} left old room: {old_room}")

    await sio.enter_room(sid, room_id)
    user_rooms[sid] = room_id

    # Tell everyone else in the room that a peer joined
    await sio.emit('peer-joined', sid, room=room_id, skip_sid=sid)
    print(f"    {sid} joined room: {room_id}")


@sio.event
async def signal(sid, data):
    to = data.get('to')
    payload = data.get('data')
    if to and payload is not None:
        await sio.emit('signal', {'from': sid, 'data': payload}, to=to)
