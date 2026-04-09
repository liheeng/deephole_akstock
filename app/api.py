# app/api.py

import os
import io
import csv
import time
from typing import Optional, List
from contextlib import asynccontextmanager, closing
from fastapi import FastAPI, HTTPException, WebSocket, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
import subprocess
import asyncio
from pydantic import BaseModel
import pandas as pd

from core.task_manager import task_manager
from db.db_common import DB
from utils.common import is_running_in_docker
from utils.log_manager import get_logger
from utils.task_util import create_sync_daily_task
from core.scheduler import run_task
from core.worker import start_workers
from db.duckdb import DuckDBController
from sources.ifind.ifind_api import IFinDApi
from sources.data_source import DataSourceApiName

# !!! Register executors, any new executor needs to be import here,
# it is very important,otherwise the API won't know how to handle
# the incoming jobs!!!
import executors.cn_daily_sync_executor    # noqa
import executors.hk_daily_sync_executor    # noqa
import executors.us_daily_sync_exectuor    # noqa

logger = get_logger(__name__)

# Init DuckDB 
db_controller = DuckDBController(db_path=DB)
logger.info("DuckDB connection initialized")


def init():
    logger.info("API service is starting up")
    # Init iFinD API
    try:
        IFinDApi(refresh_token=os.getenv("IFIND_REFRESH_TOKEN"))
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
def call_task(
    sync_type: str,
    # ✅ 正确：GET 请求用 Query 参数接收
    data_source: str = Query(DataSourceApiName.AKSHARE_SINA_API.value, description="数据源：ifind/akshare.sina/akshare.eastmoney/akshare.tencent/yfinance")
):
    try:
        # 传入数据源
        task = create_sync_daily_task(sync_type, data_source=DataSourceApiName(data_source))
        if task:
            if run_task(task):
                return {"message": f"started sync task {sync_type}, 数据源: {data_source}"}
            else:
                raise HTTPException(status_code=400, detail="启动任务失败")
        else:
            return {"message": f"无效的同步类型: {sync_type}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}")


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


class SQLQuery(BaseModel):
    sql: str


@app.post("/execute_sql")
def execute_sql(query: SQLQuery):
    try:
        logger.info("received request for executing sql: %s", query.sql)

        sql = query.sql.strip().lower()

        if sql.startswith("select") or sql.startswith("with"):
            df = db_controller.read(sql=query.sql, fetch_mode="df")
        else:
            df = db_controller.execute(sql=query.sql, fetch_mode="df")
            # return {"status": "success", "data": None, "columns": []}

        if df is None or len(df) == 0:
            return {"status": "success", "data": None, "columns": []}

        # ✅ 关键修复
        import json
        data = json.loads(df.to_json(orient="records", date_format="iso"))

        return {
            "status": "success",
            "data": data,
            "columns": df.columns.tolist()
        }

    except Exception as e:
        logger.exception(f"failed to execute sql: {query.sql}")
        return {"status": "error", "message": str(e)}


class ExportRequest(BaseModel):
    columns: List[str]
    where_sql: Optional[str] = None
    export_format: str = "csv"


@app.post("/export/stream")
async def export_stream(req: ExportRequest):
    cols = req.columns
    where = req.where_sql
    fmt = req.export_format.lower()

    def generate():
        # 🔥 安全关闭连接
        with closing(db_controller._get_connection()) as con:
            sql = f"SELECT {','.join(cols)} FROM stock_daily"
            if where and where.strip():
                sql += f" WHERE {where.strip()}"

            # CSV
            if fmt == "csv":
                yield ','.join(cols) + '\n'
                res = con.execute(sql)
                while True:
                    rows = res.fetchmany(10000)
                    if not rows:
                        break
                    for r in rows:
                        yield ','.join(str(x) if x is not None else '' for x in r) + '\n'

            # PARQUET
            elif fmt == "parquet":
                tmp = f"/tmp/exp_{int(time.time()*1000)}.parquet"
                try:
                    con.execute(f"COPY ({sql}) TO '{tmp}' (FORMAT PARQUET)")
                    with open(tmp, "rb") as f:
                        while chunk := f.read(1024 * 1024):
                            yield chunk
                finally:
                    if os.path.exists(tmp):
                        os.remove(tmp)

    media_type = "text/csv" if fmt == "csv" else "application/octet-stream"
    return StreamingResponse(
        generate(),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename=stock_daily.{fmt}"}
    )

    
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
