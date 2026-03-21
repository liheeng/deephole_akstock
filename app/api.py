# app/api.py

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
import subprocess
import asyncio
import threading
import duckdb

from task_runner import run_fetch
from app.utils.task_manager_old import task_manager
from db.db_common import DB
from utils.common import is_running_in_docker
from utils.log_manager import get_default_logger, get_task_logger
from core.task import Task, Job, TaskStatus
from core.scheduler import run_task
from core.worker import start_workers
from db.duckdb import DuckDBController
from db.db_common import DB

# !!! Register executors, any new executor needs to be import here, it is very important, 
# otherwise the API won't know how to handle the incoming jobs !!!
import executors.cn_daily_sync_executor
import executors.hk_daily_sync_executor
import executors.us_daily_sync_exectuor
import executors.download_executor
import executors.duckdb_executor

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

@app.get("/status")
def status():
    return task_manager.get_status()


@app.post("/fetch")
def trigger_fetch():

    if task_manager.running:
        return {
            "status": "rejected",
            "task_id": task_manager.task_id,   
            "running_by": task_manager.source
        }

    threading.Thread(target=run_fetch, args=("api",)).start()

    return {"status": "started"}

@app.post("/call_task")
def call_task(task: Task):
    # jobs = [Job(**j) for j in task["jobs"]]
    # t = Task(id=task["id"], jobs=jobs)
    
    try:
        if run_task(task):
            task.status = TaskStatus.SUBMITTED
            return {"task_id": task.id, "status": TaskStatus.SUBMITTED}
        else:
            return {"task_id": task.id, "status": TaskStatus.SUSPENDED}
    except Exception as e:
        return {"task_id": task.id, "status": TaskStatus.FAILED, "message": e}
    

@app.get("/tasks")
def list_tasks(limit: int = 20):

    con = duckdb.connect(DB)

    rows = con.execute(f"""
        SELECT *
        FROM task_log
        ORDER BY start_time DESC
        LIMIT {limit}
    """).fetchall()

    con.close()

    return rows


@app.get("/tasks/{task_id}")
def get_task(task_id: int):

    con = duckdb.connect(DB)

    row = con.execute("""
        SELECT *
        FROM task_log
        WHERE id=?
    """, (task_id,)).fetchone()

    con.close()

    return row

# @app.get("/logs/{task_id}")
# def get_logs(task_id: str):

#     path = f"/logs/{task_id}.log" if is_running_in_docker() else f"./logs/{task_id}.log"

#     if not os.path.exists(path):
#         return {"logs": []}

#     with open(path) as f:
#         return {"logs": f.readlines()}
    
# @app.get("/logs/{task_id}/tail")
# def tail_logs(task_id: str, n: int = 50):

#     path = f"/logs/{task_id}.log" if is_running_in_docker() else f"./logs/{task_id}.log"

#     if not os.path.exists(path):
#         return {"logs": []}

#     with open(path) as f:
#         lines = f.readlines()

#     return {"logs": lines[-n:]}

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