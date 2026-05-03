from fastapi import WebSocket
from core.log_stream import subscribe_logs
from fastapi.responses import StreamingResponse


async def log_ws(websocket: WebSocket, job_id: str):
    await websocket.accept()

    async for log in subscribe_logs(job_id):
        await websocket.send_json(log)


async def log_stream(job_id: str):
    async def event_generator():
        async for log in subscribe_logs(job_id):
            yield f"data: {log}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")