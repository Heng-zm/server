import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
socket_app = socketio.ASGIApp(sio, app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@sio.event
async def connect(sid, environ):
    print(f"User connected: {sid}")

@sio.event
async def disconnect(sid):          # ← only 1 argument (sid), no reason arg
    print(f"User disconnected: {sid}")
    await sio.emit('peer-left', sid)  # ← emit() not broadcast()

@sio.event
async def join_room(sid, room_id):
    await sio.enter_room(sid, room_id)
    await sio.emit('peer-joined', sid, room=room_id, skip_sid=sid)
    print(f"{sid} joined room: {room_id}")

@sio.event
async def signal(sid, data):
    to = data.get('to')
    payload = data.get('data')
    await sio.emit('signal', {'from': sid, 'data': payload}, to=to)
