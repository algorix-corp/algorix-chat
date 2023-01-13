import asyncio

from broadcaster import Broadcast
from fastapi import FastAPI, WebSocket
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

app = FastAPI()
broadcast = Broadcast("memory://")


class MessageEvent(BaseModel):
    channel: str
    username: str
    message: str


async def receive_message(websocket: WebSocket, channel: str, username: str):
    async with broadcast.subscribe(channel=channel) as subscriber:
        async for event in subscriber:
            message_event = MessageEvent.parse_raw(event.message)
            if message_event.username != username:
                await websocket.send_json(message_event.dict())


async def send_message(websocket: WebSocket, channel: str, username: str):
    data = await websocket.receive_text()
    event = MessageEvent(channel=channel, username=username, message=data)
    await broadcast.publish(channel=channel, message=event.json())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, channel: str, username: str):
    await websocket.accept()
    try:
        while True:
            await asyncio.gather(
                receive_message(websocket, channel, username),
                send_message(websocket, channel, username),
            )
    except WebSocketDisconnect:
        await broadcast.publish(channel=channel, message=f"{username} left the room")
        await websocket.close()


@app.on_event("startup")
async def startup():
    await broadcast.connect()


@app.on_event("shutdown")
async def shutdown():
    await broadcast.disconnect()
