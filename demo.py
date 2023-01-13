import asyncio

from broadcaster import Broadcast
from fastapi import FastAPI, WebSocket
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

app = FastAPI()
broadcast = Broadcast("memory://")


@app.on_event("startup")
async def startup():
    await broadcast.connect()


@app.on_event("shutdown")
async def shutdown():
    await broadcast.disconnect()


class MessageEvent(BaseModel):
    username: str
    room: str
    message: str


async def receive_message(websocket: WebSocket, username: str, room: str):
    async with broadcast.subscribe(channel=room) as subscriber:
        async for event in subscriber:
            message_event = MessageEvent.parse_raw(event.message)
            if message_event.username != username:
                await websocket.send_json(message_event.dict())


async def send_message(websocket: WebSocket, username: str, room: str):
    data = await websocket.receive_text()
    event = MessageEvent(username=username, message=data, room=room)
    await broadcast.publish(channel=room, message=event.json())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, username: str, room: str):
    await websocket.accept()
    try:
        while True:
            await asyncio.gather(
                receive_message(websocket, username, room),
                send_message(websocket, username, room),
            )
    except WebSocketDisconnect:
        await broadcast.publish(channel=room, message=f"{username} left the room")
        await websocket.close()
