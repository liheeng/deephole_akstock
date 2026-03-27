# app/api.py

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.staticfiles import StaticFiles
import subprocess
import asyncio

from core.task_manager import task_manager
from db.db_common import DB
from utils.common import is_running_in_docker
from utils.log_manager import get_logger
from utils.task_util import create_sync_daily_task
from core.scheduler import run_task
from core.worker import start_workers
from db.duckdb import DuckDBController
from core.error import TaskError
from sources.ifind.ifind_api import IfindApi

# !!! Register executors, any new executor needs to be import here,
# it is very important,otherwise the API won't know how to handle
# the incoming jobs!!!
import executors.cn_daily_sync_executor    # noqa
import executors.hk_daily_sync_executor    # noqa
import executors.us_daily_sync_exectuor    # noqa

logger = get_logger(__name__)


def init():
    logger.info("API service is starting up")

    # Init DuckDB 
    DuckDBController(db_path=DB)
    logger.info("DuckDB connection initialized")

    # Init iFinD API
    try:
        IfindApi(refresh_token=os.getenv("IFIND_REFRESH_TOKEN"))
        logger.info("iFinD API initialized")
    except Exception as e:
        logger.error(f"iFinD API is failed to initialize, error: str({e})")
    
    # 启动工作线程池
    start_workers(n=4)   # 👈 在这里启动
    logger.info("Worker threads started")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init()
    yield
    logger.error("API servide is shutting down")

app = FastAPI(lifespan=lifespan)

if is_running_in_docker():
    app.mount("/terminal", StaticFiles(directory="terminal", html=True), name="terminal")


@app.get("/sync_daily/{sync_type}")
def call_task(sync_type: str):
    try:
        task = create_sync_daily_task(sync_type)
        if task:

            if run_task(task):
                return {"message": "started sync task-" + sync_type}
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"failed to start sync task-{sync_type}"
                )
        else:
            return {"message": "invalid sync type-" + sync_type}
    except Exception as e:
        if isinstance(e, TaskError):
            return {"message": e.message}
        else:    
            raise HTTPException(
                status_code=500,
                detail=f"failed to run task-{sync_type}, error：{str(e)}"
            )


@app.get("/tasks")
def list_tasks(limit: int = 20):
    try:
        return task_manager.list_tasks(limit)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"failed to list tasks, error：{str(e)}"
        )


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    return task_manager.load_task(task_id)



@app.get("/logs/tail")
def tail_logs(n: int = 50):

    path = "/logs/default.log" if is_running_in_docker() else "./logs/default.log"

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