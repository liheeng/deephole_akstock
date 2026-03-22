# app/api.py

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
import subprocess
import asyncio
import threading
import duckdb

from core.task_manager import task_manager
from db.db_common import DB
from utils.common import is_running_in_docker
from utils.log_manager import get_default_logger, get_task_logger
from utils.task_util import create_sync_daily_task
from core.task import Task, TaskStatus
from core.job import JobType
from core.scheduler import run_task
from core.worker import start_workers
from db.duckdb import DuckDBController
from db.db_common import DB

# !!! Register executors, any new executor needs to be import here, it is very important, 
# otherwise the API won't know how to handle the incoming jobs !!!
import executors.cn_daily_sync_executor
import executors.hk_daily_sync_executor
import executors.us_daily_sync_exectuor

logger = get_default_logger()

def init():
    logger.info("API service is starting up")
    
    # Init DuckDB 
    DuckDBController(db_path=DB)
    logger.info("DuckDB initialized")

    # 启动工作线程池
    start_workers(n=4)   # 👈 在这里启动
    logger.info("Worker threads started")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init()
    yield
    get_default_logger().error("API servide is shutting down")

app = FastAPI(lifespan=lifespan)

if is_running_in_docker():
    app.mount("/terminal", StaticFiles(directory="terminal", html=True), name="terminal")

# @app.get("/status")
# def status():
#     return task_manager.get_status()


# @app.post("/fetch")
# def trigger_fetch():

#     if task_manager.running:
#         return {
#             "status": "rejected",
#             "task_id": task_manager.task_id,   
#             "running_by": task_manager.source
#         }

#     threading.Thread(target=run_fetch, args=("api",)).start()

#     return {"status": "started"}

@app.get("/sync_daily/{sync_type}")
def call_task(sync_type: str):
    task = create_sync_daily_task(sync_type)
    if task:
        try:
         if run_task(task):
            return {"message": "started sync task-" + sync_type}
         else:
            return {"message": "failed to start sync task-" + sync_type}        
        except Exception as e:
            return {"error": str(e)}
    else:
        return {"error":"wrong sync type!"}
    
@app.get("/tasks")
def list_tasks(limit: int = 20):
    return task_manager.list_tasks(limit)

@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    return task_manager.load_task(task_id)


# @app.get("/logs/{task_id}")
# def get_logs(task_id: str):

#     path = f"/logs/{task_id}.log" if is_running_in_docker() else f"./logs/{task_id}.log"

#     if not os.path.exists(path):
#         return {"logs": []}

#     with open(path) as f:
#         return {"logs": f.readlines()}
    
@app.get("/logs/tail")
def tail_logs(n: int = 50):

    path = f"/logs/default.log" if is_running_in_docker() else f"./logs/default.log"

    if not os.path.exists(path):
        return {"logs": []}

    with open(path) as f:
        lines = f.readlines()

    return {"logs": lines[-n:]}

@app.websocket("/ws/terminal/{container}")
async def terminal_ws(websocket: WebSocket, container: str):

    await websocket.accept()

    # 启动 docker shell
    process = subprocess.Popen(
        ["docker", "exec", "-i", container, "/bin/sh"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    async def read_output():
        while True:
            line = process.stdout.readline()
            if line:
                await websocket.send_text(line)
            await asyncio.sleep(0.01)

    asyncio.create_task(read_output())

    try:
        while True:
            cmd = await websocket.receive_text()
            process.stdin.write(cmd + "\n")
            process.stdin.flush()

    except Exception:
        process.kill()