# app/api.py

import os
import pty
import asyncio
import json
import time
import paramiko
import docker
from typing import Optional, List, Any, Dict
from contextlib import asynccontextmanager, closing
from fastapi import FastAPI, HTTPException, WebSocket, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
import numpy as np
import traceback
import re
from datetime import datetime, date
import nanoid
from starlette.websockets import WebSocketDisconnect
import fcntl
import subprocess
import atexit
import psutil
import socket

from fastapi.middleware.cors import CORSMiddleware
from api_config import CORS_CONFIG

from utils.log_manager import init_logger
from core.task_manager import task_manager
from db.db_common import DB
from utils.common import is_running_in_docker
from loguru import logger
from utils.task_util import create_sync_daily_task, create_python_scripts_task
from core.scheduler import run_task
from core.worker import start_workers
from db.duckdb import DuckDBController, DuckDBTransaction
from sources.ifind.ifind_api import IFinDApi
from sources.data_source import DataSourceApiName
from core.log_stream import log_queues

from backtest.backtest_base import ApiDataSetConfig, ApiBacktestRequest
from vectorbt_test.core.registry import NodeRegistry
from vectorbt_test.core.portfolio import PortfolioParameters, PortfolioResultWrapper
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.engine.portfolio_builder import PortfolioBuilder
from vectorbt_test.engine.init import load_register_nodes
from vectorbt_test.portfolios.signal_strategy_portfolio import StrategyOp
from executors.base import ExecutorBase
from core.job import JobType, Job
from executors.base import get_executor

# !!! Register executors, any new executor needs to be import here,
# it is very important,otherwise the API won't know how to handle
# the incoming jobs!!!
import executors.cn_daily_sync_executor    # noqa
import executors.hk_daily_sync_executor    # noqa
import executors.us_daily_sync_exectuor    # noqa
import executors.python_script_executor


# Init logger
init_logger()

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
    
    load_register_nodes()
    logger.info("load backtest's register nodes")
    # 启动工作线程池
    start_workers(n=4)   # 👈 在这里启动
    logger.info("Worker threads started")

    # logger.info("try to allocate numba and vbt cpu cores")
    # try:
    #     allocate_cpu_to_numba_vbt()
    # except Exception as e:
    #     logger.error(f"allocate numba and vbt cpu cores is failed, error: str({e})")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init()
    yield
    logger.error("API servide is shutting down")


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    **CORS_CONFIG
)

if is_running_in_docker():
    app.mount("/terminal", StaticFiles(directory="terminal", html=True), name="terminal")


@app.on_event("startup")
def debug_routes():
    logger.info("\n===== ROUTES =====")
    for r in app.routes:
        logger.info(r.path)


@app.get("/api/sync_daily/{sync_type}")
def call_task(
    sync_type: str,
    # ✅ 正确：GET 请求用 Query 参数接收
    data_source: str = Query(DataSourceApiName.AKSHARE_SINA_API.value, description="数据源：ifind/akshare.sina/akshare.eastmoney/akshare.tencent/yfinance")
):
    try:
        logger.info(f"received sync daily request {sync_type}")
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
        logger.exception(e)
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}")


class ScriptRequest(BaseModel):
    script: str


@app.post("/api/task/execute_script_job")
def execute_script_job(req: ScriptRequest):
    script = req.script
    try:
        logger.info(f"received execute script job request: scripts: {script}")
        # 传入数据源
        task = create_python_scripts_task(script)
        if task:
            if run_task(task):
                return {"status": "success", "task_id": f"{task.id}", "job_id": f"{task.jobs[0].id}", "job_type": f"{task.jobs[0].type.value}", "message": f"started script task {task.id}"}
            else:
                raise HTTPException(status_code=400, detail="启动任务失败")
        else:
            return {"status": "failed", "message": "Failed to create script execute task"}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}")


class JobMeta(BaseModel):
    job_id: str
    job_type: str


@app.post("/api/task/cancel_script_job")
def cancel_script_job(job_meta: JobMeta):
    try:
        logger.info(f"received cancel script job request: scripts: {job_meta}")
        # 传入数据源
        executor: ExecutorBase = get_executor(JobType(job_meta.job_type))
        if executor.cancel_job(job_meta.job_id):
            return {"status": "success", "job_id": f"{job_meta.job_id}", "job_type": f"{job_meta.job_type}", "message": f"cancelled job {job_meta.job_id}"}
        else:
            return {"status": "failed", "job_id": f"{job_meta.job_id}", "job_type": f"{job_meta.job_type}", "message": f"Failed to cancel script execute job {job_meta.job_id}"}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}")
    
@app.get("/api/tasks")
def list_tasks(limit: int = 20):
    try:
        logger.info(f"received get tasks request, limit: {limit}")
        tasks = task_manager.list_tasks(limit)
        logger.info(f"list tasks: {tasks}")
        return tasks
    except Exception as e:
        logger.exception(e)
        raise HTTPException(
            status_code=400,
            detail=f"failed to list tasks, error：{str(e)}"
        )


@app.get("/api/tasks/{task_id}")
def get_task(task_id: str):
    logger.info(f"received get task request, task id: {task_id}")
    return task_manager.load_task(task_id)


LOG_PATH = "/logs/default.log" if is_running_in_docker() else "./logs/default.log"


@app.get("/api/logs/tail")
def tail_logs(n: int = 50):
    logger.info(f"received get app log request, line: {n}")
    if not os.path.exists(LOG_PATH):
        return {"logs": []}

    with open(LOG_PATH) as f:
        lines = f.readlines()

    return {"logs": lines[-n:]}


@app.websocket("/api/ws/logs/default")
async def default_log_ws(websocket: WebSocket, level: str = "", keyword: str = ""):
    logger.info(f"received get app log webstock request, webstocket: {websocket}, level: {level}, keyword: {keyword}")
    await websocket.accept()
    last_pos = 0

    while True:
        if not os.path.exists(LOG_PATH):
            await asyncio.sleep(1)
            continue

        with open(LOG_PATH, "r", encoding="utf-8") as f:
            f.seek(last_pos)
            lines = f.readlines()
            last_pos = f.tell()

        for line in lines:
            # ===================== 修复：适配你真实的日志格式 =====================
            ts = ""
            lv = "INFO"
            msg = ""

            try:
                # 按 | 分割：[时间, 等级, 剩余内容]
                parts = line.strip().split(" | ", 2)
                if len(parts) == 3:
                    ts = parts[0].strip()                     # 时间
                    lv = parts[1].strip()                      # 等级
                    msg_part = parts[2].strip()                # 模块:行号 - 消息

                    # 把后面的内容也一起当消息展示（更友好）
                    msg = msg_part
            except Exception:
                # 解析失败就原样输出
                ts = ""
                lv = "INFO"
                msg = line.strip()

            # ===================== 过滤逻辑 =====================
            if level and lv != level:
                continue
            if keyword and keyword.lower() not in msg.lower():
                continue

            # ===================== 发送（永远不会报错） =====================
            await websocket.send_json({
                "timestamp": ts,
                "level": lv,
                "message": msg
            })

        await asyncio.sleep(0.5)


class SQLQuery(BaseModel):
    sql: str


@app.post("/api/execute_sql")
def execute_sql(query: SQLQuery):
    try:
        logger.info(f"received request for executing sql: {query.sql}")

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
        logger.exception(f"failed to execute sql: {query.sql}, err: {e}")
        return {"status": "error", "message": str(e)}


class ExportRequest(BaseModel):
    columns: List[str]
    where_sql: Optional[str] = None
    export_format: str = "csv"


def validate_sql(sql: str):
    if not sql:
        raise ValueError("SQL 不能为空")

    s = sql.strip().lower()

    # ======================
    # 1. 只允许 SELECT
    # ======================
    if not s.startswith("select"):
        raise ValueError("只允许 SELECT 查询")

    # ======================
    # 2. 禁止多语句
    # ======================
    if ";" in s:
        raise ValueError("不允许多语句执行")

    # ======================
    # 3. 禁止危险关键字
    # ======================
    forbidden = [
        "insert", "update", "delete",
        "drop", "alter", "truncate",
        "create", "replace"
    ]

    for word in forbidden:
        if re.search(rf"\b{word}\b", s):
            raise ValueError(f"SQL 包含非法关键字: {word}")

    # ======================
    # 4. 禁止注释绕过
    # ======================
    if "--" in s or "/*" in s:
        raise ValueError("不允许 SQL 注释")

    return True


def build_sql(req: dict, for_count=False):
    cols = ", ".join(req["columns"]) if not for_count else "COUNT(*) as cnt"

    sql = f"SELECT {cols} FROM stock_daily WHERE 1=1"

    if req.get("where"):
        sql += f" AND {req['where']}"

    if req.get("group_by"):
        sql += f" GROUP BY {req['group_by']}"

    if req.get("order_by") and not for_count:
        sql += f" ORDER BY {req['order_by']}"

    if req.get("limit") and not for_count:
        sql += f" LIMIT {req['limit']}"

    return sql


ALLOWED_COLUMNS = {
    "symbol", "symbol_name", "market", "date",
    "open", "high", "low", "close", "volume", "amount",
    "pct", "turnover", "adjust_mode", "adjust_factor"
}

MAX_LIMIT = 1_000_000


def validate_req(req: dict):
    # 字段校验
    for col in req["columns"]:
        if col not in ALLOWED_COLUMNS:
            raise ValueError(f"非法字段: {col}")

    # limit限制
    if req.get("limit"):
        if int(req["limit"]) > MAX_LIMIT:
            raise ValueError("limit过大")
        

@app.post("/api/export/preview")
def export_preview(req: dict):
    logger.info(f"received export preview request, {req}")
    validate_req(req)
    _req = {**req}
    if _req.get("limit") and int(_req["limit"]) > 50:
        _req["limit"] = 50
    sql = build_sql(_req)
    validate_sql(sql)

    with closing(db_controller._get_connection()) as con:
        # 查询数据
        
        cursor = con.execute(sql)
        rows = cursor.fetchall()
        
        # ✅ 关键修复：description 不加 ()
        columns = [desc[0] for desc in cursor.description]

        # 查询总数
        count_sql = build_sql(_req, for_count=True)
        total = con.execute(count_sql).fetchone()
        total = total[0] if total else 0

        return {
            "rows": [dict(zip(columns, row)) for row in rows],
            "total": total
        }


@app.post("/api/export/stream")
async def export_stream(req):
    logger.info(f"received export data stream request, {req}")
    validate_req(req)
    cols = req["columns"]
    fmt = req.get("export_format", "csv").lower()
    sql = build_sql(req)
    validate_sql(sql)

    def generate():
        # 🔥 安全关闭连接
        with closing(db_controller._get_connection()) as con:            
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
    try:
        return StreamingResponse(
            generate(),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename=stock_daily.{fmt}"}
        )
    except Exception as e:
        logger.exception(f"failed to export stream, error: {str(e)}")
        # raise HTTPException(status_code=500, detail=str(e))
        return {"status": "error", "message": str(e)}


@app.get("/api/nodes")
def get_nodes():
    logger.info("received get registry nodes request")
    nodes = NodeRegistry.to_dict()
    # logger.info(f"nodes: {nodes}")
    logger.info(f"get all nodes: {nodes.keys()}")
    return nodes


def load_data_somehow(ds: ApiDataSetConfig) -> pd.DataFrame:
    sql = build_dataset_sql(ds.sourceDef.model_dump(exclude_none=False))
    logger.info(f"built sql for backtest data source: {sql}")
    # from db.stock_daily_util import get_symbol_data, get_symbols_data
    db_controller = DuckDBController(db_path="../data/stock.duckdb")
    # df = get_symbols_data(db_controller, "603259.SH, 600362.SH", "2025-01-01", "2026-03-31")
    # df = get_symbol_data(db_controller, "603259.SH", "2025-01-01", "2026-03-31")
    df = db_controller.read(sql, fetch_mode='df')
    return df


class BacktestResultRow(BaseModel):
    id: str
    dataset_config_id: str
    portfolio_name: str
    stats: Dict[str, Any]
    equity: Dict[str, Any]
    trades: List[Dict[str, Any]]
    created_at: Optional[datetime] = None


def json_serial(obj):
    """
    针对 vectorbt 和 pandas 结果的万能 JSON 序列化器
    涵盖: Timestamp, Timedelta, NaT, NaN, Inf, NumPy types
    """
    # 1. 处理空值 (Pandas 的 NaT 和 NaN)
    if pd.isna(obj):
        return None

    # 2. 处理日期和时间 (Timestamp, datetime, date)
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()

    # 3. 处理时间间隔 (Timedelta / Offset)
    if isinstance(obj, (pd.Timedelta, pd.tseries.offsets.BaseOffset)):
        return str(obj)

    # 4. 处理数字 (NumPy 标量)
    if isinstance(obj, (np.integer, np.floating)):
        # 检查 NumPy 特有的 inf
        if np.isinf(obj):
            return None
        return obj.item()  # 转换为 Python 原生 int 或 float

    # 5. 处理数组/序列 (ndarray, Series, Index)
    if isinstance(obj, (np.ndarray, pd.Series, pd.Index)):
        return obj.tolist()

    # 6. 处理字典 (如果有嵌套的话)
    if isinstance(obj, dict):
        return {str(k): json_serial(v) for k, v in obj.items()}

    # 7. 处理 Python 原生的 inf / -inf
    if isinstance(obj, float):
        if obj == float('inf') or obj == float('-inf'):
            return None

    raise TypeError(f"Type {type(obj)} not serializable")


def save_backtest_result(
    dataset_config_id: str,
    portfolio_name: str,
    stats: dict | None,
    equity: dict,
    trades: list
):
    conn = db_controller
    try:
        result_id = f"bt_result_{nanoid.generate()}"

        conn.execute("""
            INSERT INTO backtest_portfolio_results (
                id, dataset_config_id, portfolio_name, stats, equity, trades
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, [
            result_id,
            dataset_config_id,
            portfolio_name,
            json.dumps(stats, ensure_ascii=False, default=json_serial),
            json.dumps(equity, ensure_ascii=False, default=json_serial),
            json.dumps(trades, ensure_ascii=False, default=json_serial)
        ])

        return result_id
    except Exception as e:
        logger.exception(f"failed to save backtest result, error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/backtest")
def run_backtest(req: ApiBacktestRequest):
    try:
        logger.info(f"received run backtest request, {req}")
        # 1. 构建 Portfolio
        portfolio_config = req.portfolio_config
        builder = PortfolioBuilder.new(portfolio_config.name, portfolio_config.portfolio_mode)

        if portfolio_config.strategies is not None and len(portfolio_config.strategies) > 0:
            for s in portfolio_config.strategies.values():
                builder.add_strategy(s.name)

                if s.factorIds is not None and len(s.factorIds) > 0:
                    for fid in s.factorIds:
                        factor = portfolio_config.factors.get(fid)  # 修复
                        if factor is not None:
                            ff = {"id": factor.id, "name": factor.name, "expr": factor.expr}
                            builder.add_factor(ff)

                if s.signalId is not None:
                    signal = portfolio_config.signals.get(s.signalId)  # 修复
                    if signal is not None:
                        sig = {"id": signal.id, "name": signal.name, "expr": signal.expr}
                        builder.set_strategy_signal(sig)

                # 全部修复：用 .get() 防止 KeyError
                if s.config is not None and s.config.get("mode") is not None:
                    builder.set_strategy_mode(s.config.get("mode"))

                if s.config is not None and s.config.get("threshold") is not None:
                    builder.set_strategy_threshold(s.config.get("threshold"))

                if s.config is not None and s.config.get("top_n") is not None:
                    builder.set_strategy_top_n(s.config.get("top_n"))

                builder.end_strategy()

        # 修复：.get()
        if portfolio_config.strategy_op is not None and portfolio_config.strategy_op.get("enabled") is True:
            builder.set_strategy_op((portfolio_config.strategy_op.get("value") or StrategyOp.OR.value).upper())

        # 修复：.get()
        if portfolio_config.schedule_signal and portfolio_config.schedule_signal.get("enabled") is True:
            signal_id = portfolio_config.schedule_signal.get("signalId")
            signal = portfolio_config.signals.get(signal_id)
            if signal is not None:
                sig = {"id": signal.id, "name": signal.name, "expr": signal.expr}
                builder.set_schedule_signal(sig)

        # 修复：.get()
        if portfolio_config.vote_weights is not None and portfolio_config.vote_weights.get("enabled") is True:
            builder.vote_weights(portfolio_config.vote_weights.get("value"))

        if portfolio_config.strategy_weights is not None and portfolio_config.strategy_weights.get("enabled") is True:
            builder.strategy_weights(portfolio_config.strategy_weights.get("value"))

        builder.set_portfolio_params(
            PortfolioParameters(**portfolio_config.params)
        )

        portfolio = builder.build()

        # 2. 加载数据
        df = load_data_somehow(req.dataset_config)

        # 3. 运行回测
        pf = portfolio.run(DataProvider(None), df)
        pfwrapper = PortfolioResultWrapper(pf)

        equity_curve: Any = pfwrapper.equity_values()
        stats: Any = pfwrapper.stats_values()
        trades: Any = pfwrapper.trades_values()
        

        # ==========================
        # ✅ 保存到数据库
        # ==========================
        # equity_curve_dict = json.loads(equity_curve) if isinstance(equity_curve, str) else equity_curve
        save_backtest_result(
            dataset_config_id=req.dataset_config.id,
            portfolio_name=portfolio_config.name,
            stats=stats,
            equity=equity_curve,
            trades=trades
        )

        return {
            "stats": stats,
            "equity": equity_curve,
            "trades": trades
        }
    except Exception as e:
        logger.exception(f"failed to run backtest\n{e}")
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}\n{traceback.format_exc()}")


@app.get("/api/backtest/results", response_model=List[BacktestResultRow])
def query_backtest_results(
    dataset_config_id: Optional[str] = None,
    portfolio_name: Optional[str] = None
):
    conn = db_controller
    try:
        logger.info("received get backtest results request, dataset_config_id: {dataset_config_id}, portfolio_name: {portfolio_name} ")
        sql = """
            SELECT id, dataset_config_id, portfolio_name,
                   stats, equity, trades, created_at
            FROM backtest_portfolio_results
            WHERE 1=1
        """
        params = []

        if dataset_config_id:
            sql += " AND dataset_config_id = ?"
            params.append(dataset_config_id)

        if portfolio_name:
            sql += " AND portfolio_name = ?"
            params.append(portfolio_name)

        sql += " ORDER BY created_at DESC"

        rows = conn.execute(sql, params, fetch_mode="all")
        columns = [
            "id", "dataset_config_id", "portfolio_name",
            "stats", "equity", "trades", "created_at"
        ]

        result = []
        for row in rows:
            item = dict(zip(columns, row))
            item["stats"] = json.loads(item["stats"])
            item["equity"] = json.loads(item["equity"])
            item["trades"] = json.loads(item["trades"])

            item["created_at"] = item["created_at"].isoformat()
            result.append(item)

        return result

    except Exception as e:
        logger.exception(f"failed to query backtest results, error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def materialize_dataset(dataset_id, source_def):

    sql = build_dataset_sql(source_def)

    table_name = f"dataset_{dataset_id}"

    db_controller.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        {sql}
    """)

    return {
        "table": table_name,
        "row_count": db_controller.read(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    }


def build_dataset_sql(source_def: dict):
    if source_def["type"] == "sql":
        return source_def["sql"]

    sql = "SELECT * FROM stock_daily WHERE 1=1"

    if source_def.get("markets"):
        markets = ",".join([f"'{s}'" for s in source_def["markets"]])
        sql += f" AND market IN ({markets})"

    if source_def.get("symbols"):
        symbols = ",".join([f"'{s}'" for s in source_def["symbols"]])
        sql += f" AND symbol IN ({symbols})"

    if source_def.get("sectors"):
        sectors = ",".join([f"'{s}'" for s in source_def["sectors"]])
        sql += f" AND sector IN ({sectors})"

    if source_def.get("universe"):
        sql += f" AND symbol IN (SELECT symbol FROM universe_map WHERE name = '{source_def['universe']}')"

    sql += f" AND date BETWEEN '{source_def['start']}' AND '{source_def['end']}'"

    return sql


# ==============================
# 请求模型（前端传过来的结构）
# ==============================
class DatasetCreateRequest(BaseModel):
    id: str
    name: str
    sourceDef: Dict[str, Any]
    schema: Optional[List[str]] = None
    rowCount: Optional[int] = None
    cache: Optional[Dict[str, Any]] = None


# ==============================
# 自动创建 or 更新 dataset
# ==============================
@app.get("/api/backtest/datasets", response_model=List[Any])
def backtest_fetch_datasets():
    try:
        logger.info("received get backtest datasets request")
        with DuckDBTransaction(db_controller) as tx:

            rows = tx.read("""
                SELECT 
                    id, 
                    name, 
                    createdAt, 
                    sourceDef, 
                    schema, 
                    rowCount, 
                    cache
                FROM datasets
                ORDER BY createdAt DESC
            """, fetch_mode="all")

        if not rows:
            return []

        def parse_json(x, default):
            if not x:
                return default
            try:
                return json.loads(x)
            except:
                return default

        result = []

        for r in rows:
            item = {
                "id": r[0],
                "name": r[1],
                "createdAt": r[2].isoformat() if r[2] else None,
                "sourceDef": parse_json(r[3], {}),
                "schema": parse_json(r[4], None),
                "rowCount": r[5],
                "cache": parse_json(r[6], None),
            }
            result.append(item)

        return result

    except Exception as e:
        logger.exception(f"failed to fetch datasets, {e}")
        raise HTTPException(
            status_code=500,
            detail=f"错误: {str(e)}\n{traceback.format_exc()}"
        )
    

@app.post("/api/backtest/dataset")
def backtest_update_dataset(req: DatasetCreateRequest):

    now = datetime.now()
    try:
        logger.info("received update backtest dataset request, {req}")
        with DuckDBTransaction(db_controller) as tx:

            sql = """
                INSERT INTO datasets (
                    id, name, createdAt, updatedAt,
                    sourceDef, schema, rowCount, cache
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    updatedAt = excluded.updatedAt,
                    sourceDef = excluded.sourceDef,
                    schema = excluded.schema,
                    rowCount = excluded.rowCount,
                    cache = excluded.cache
            """

            tx.execute(sql, [
                req.id,
                req.name,
                now,
                now,
                json.dumps(req.sourceDef, ensure_ascii=False),
                json.dumps(req.schema) if req.schema else None,
                req.rowCount,
                json.dumps(req.cache) if req.cache else None
            ])

        return {
            "success": True,
            "message": "保存成功",
            "id": req.id
        }

    except Exception as e:
        logger.exception(f"failed to create/update dataset {e}")
        raise HTTPException(500, str(e))


# ==============================
# Pydantic 模型（和前端 TS 完全对应）
# ==============================

# 你的 Pydantic 模型（必须加 strategies 字段）
class BacktestConfig(BaseModel):
    id: str
    name: str
    portfolio_mode: str
    params: dict
    schedule_signal: dict
    strategy_op: dict
    vote_weights: dict
    strategy_weights: dict
    # 前端必传的策略数组
    strategies: dict
    factors: dict
    signals: dict
    created_at: str = None
    updated_at: str = None

# ==============================
# 1. 获取所有回测配置 + 关联3张表数据
# ==============================
@app.get("/api/backtest/configs", response_model=List[BacktestConfig])
def get_all_backtest_configs():
    conn = db_controller
    try:
        logger.info("received get backtest configs request")
        rows = conn.execute("""
            SELECT 
                id, name, portfolio_mode, params,
                schedule_signal, strategy_op, vote_weights, strategy_weights,
                created_at, updated_at
            FROM backtest_config
            ORDER BY created_at DESC
        """, fetch_mode="all")

        columns = [
            "id", "name", "portfolio_mode", "params",
            "schedule_signal", "strategy_op", "vote_weights", "strategy_weights",
            "created_at", "updated_at"
        ]

        result = []
        for row in rows:
            item = dict(zip(columns, row))
            
            # JSON 解析
            item["params"] = json.loads(item["params"]) if item["params"] else {}
            item["schedule_signal"] = json.loads(item["schedule_signal"]) if item["schedule_signal"] else {}
            item["strategy_op"] = json.loads(item["strategy_op"]) if item["strategy_op"] else {}
            item["vote_weights"] = json.loads(item["vote_weights"]) if item["vote_weights"] else {}
            item["strategy_weights"] = json.loads(item["strategy_weights"]) if item["strategy_weights"] else {}
            
            # 时间格式化
            if item["created_at"]:
                item["created_at"] = item["created_at"].isoformat()
            if item["updated_at"]:
                item["updated_at"] = item["updated_at"].isoformat()

            # ==========================================
            # 关联查询：strategy / factor / signal
            # 严格按你的表结构 + backtest_id 关联
            # ==========================================
            backtest_id = item["id"]


            # 1. 查询策略
            backtest_strategy_columns = [
                "id", "backtest_id", "name", "factor_ids", "signal_id", "config", "created_at"
            ]
            strategies = conn.execute(
                "SELECT id, backtest_id, name, factor_ids, signal_id, config, created_at FROM backtest_strategy WHERE backtest_id = ?",
                params=[backtest_id],
                fetch_mode="all"
            )
            # 2. 查询因子
            backtest_factor_columns = [
                "id", "backtest_id", "name", "expr", "created_at"
            ]
            factors = conn.execute(
                "SELECT id, backtest_id, name, expr, created_at FROM backtest_factor WHERE backtest_id = ?",
                params=[backtest_id],
                fetch_mode="all"
            )
            # 3. 查询信号
            backtest_signal_columns = [
                "id", "backtest_id", "name", "expr", "created_at"
            ]
            signals = conn.execute(
                "SELECT id, backtest_id, name, expr, created_at FROM backtest_signal WHERE backtest_id = ?",
                params=[backtest_id],
                fetch_mode="all"
            )

            logger.info(f"strategies: {strategies}, factors: {factors}, signals: {signals}")

            # 组装到返回结构
            if strategies is not None:
                strategies_dict = {}
                for strategy in strategies:
                    backtest_strategy_item = dict(zip(backtest_strategy_columns, strategy))
                    backtest_strategy_item["created_at"] = backtest_strategy_item["created_at"].isoformat()
                    strategies_dict[backtest_strategy_item["id"]] = backtest_strategy_item

                item["strategies"] = strategies_dict

            if factors is not None:
                factors_dict = {}
                for factor in factors:
                    backtest_factor_item = dict(zip(backtest_factor_columns, factor))
                    backtest_factor_item["created_at"] = backtest_factor_item["created_at"].isoformat()
                    factors_dict[backtest_factor_item["id"]] = backtest_factor_item

                item["factors"] = factors_dict

            if signals is not None:
                signals_dict = {}
                for signal in signals:
                    backtest_signal_item = dict(zip(backtest_signal_columns, signal))
                    backtest_signal_item["created_at"] = backtest_signal_item["created_at"].isoformat()
                    signals_dict[backtest_signal_item["id"]] = backtest_signal_item

                item["signals"] = signals_dict


            result.append(item)

        return result

    except Exception as e:
        logger.exception(f"failed to fetch backtest configs\n{e}")
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}\n{traceback.format_exc()}")


# ==============================
# 2. 自动创建或更新 backtest config（含子表保存）
# ==============================
@app.post("/api/backtest/config")
def update_backtest_config(config: BacktestConfig):
    logger.info(f"received update backtest config request, config: {config}")
    if not config.id:
        raise HTTPException(status_code=400, detail="Backtest(Portfolio) id 不能为空")

    logger.info(f"update backtest config: {config}")
    
    backtest_id = config.id

    try:
        with DuckDBTransaction(db_controller) as tx:

            # =========================
            # 1. 主表 UPSERT
            # =========================
            sql = """
                INSERT INTO backtest_config (
                    id, name, portfolio_mode, params,
                    schedule_signal, strategy_op, vote_weights, strategy_weights
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    name = excluded.name,
                    portfolio_mode = excluded.portfolio_mode,
                    params = excluded.params,
                    schedule_signal = excluded.schedule_signal,
                    strategy_op = excluded.strategy_op,
                    vote_weights = excluded.vote_weights,
                    strategy_weights = excluded.strategy_weights,
                    updated_at = now()
            """

            tx.execute(sql, [
                backtest_id,
                config.name,
                config.portfolio_mode,
                json.dumps(config.params),
                json.dumps(config.schedule_signal),
                json.dumps(config.strategy_op),
                json.dumps(config.vote_weights),
                json.dumps(config.strategy_weights)
            ])

            # =========================
            # 2. 删除旧数据
            # =========================
            tx.execute("DELETE FROM backtest_strategy WHERE backtest_id = ?", [backtest_id])
            tx.execute("DELETE FROM backtest_factor WHERE backtest_id = ?", [backtest_id])
            tx.execute("DELETE FROM backtest_signal WHERE backtest_id = ?", [backtest_id])

            # =========================
            # 3. 插入 strategy
            # =========================
            for strategy in config.strategies.values():
                tx.execute("""
                    INSERT INTO backtest_strategy
                    (id, backtest_id, name, factor_ids, signal_id, config)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    strategy.get("id"),
                    backtest_id,
                    strategy.get("name"),
                    json.dumps(strategy.get("factorIds", [])),
                    strategy.get("signalId"),
                    json.dumps(strategy.get("config", {}))
                ])

            # =========================
            # 4. 插入 factor
            # =========================
            for factor in config.factors.values():
                tx.execute("""
                    INSERT INTO backtest_factor
                    (id, backtest_id, name, expr)
                    VALUES (?, ?, ?, ?)
                """, [
                    factor.get("id"),
                    backtest_id,
                    factor.get("name"),
                    factor.get("expr")
                ])

            # =========================
            # 5. 插入 signal
            # =========================
            for signal in config.signals.values():
                tx.execute("""
                    INSERT INTO backtest_signal
                    (id, backtest_id, name, expr)
                    VALUES (?, ?, ?, ?)
                """, [
                    signal.get("id"),
                    backtest_id,
                    signal.get("name"),
                    signal.get("expr")
                ])

        # 👇 能执行到这里，说明已经自动 COMMIT
        return {
            "success": True,
            "message": "保存成功",
            "id": backtest_id
        }

    except Exception as e:
        logger.exception(f"failed to save backtest config: {config}, error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"错误: {str(e)}\n{traceback.format_exc()}"
        )


async def handle_docker(ws, container_name):
    client = docker.from_env()
    container = client.containers.get(container_name)

    exec_id = client.api.exec_create(
        container.id,
        cmd="/bin/bash",
        tty=True,
        stdin=True
    )

    sock = client.api.exec_start(exec_id, tty=True, socket=True)

    async def read():
        while True:
            data = sock.recv(1024)
            if not data:
                break
            await ws.send_text(data.decode(errors="ignore"))

    async def write():
        while True:
            msg = await ws.receive_text()

            if msg.startswith("{"):
                data = json.loads(msg)
                if data.get("type") == "resize":
                    client.api.exec_resize(exec_id, data["rows"], data["cols"])
                continue

            sock.send(msg.encode())

    await asyncio.gather(read(), write())


TERMINAL_TARGETS = [
    {
        "id": "local",
        "name": "Service Host",
        "type": "host",
        "mode": "pty",   # pty / ssh
    },
    {
        "id": "server1",
        "name": "Remote Server",
        "type": "host",
        "mode": "ssh",
        "host": "127.0.0.1",
        "username": "your_user",
        "password": "your_pass",
    },
]


@app.get("/api/terminal/targets")
def get_total_targets():
    logger.info("recevied get total targets/hosts request")

    result = TERMINAL_TARGETS.copy()
    try:
        client = docker.from_env()
        containers = client.containers.list()

        for c in containers:
            result.append({
                "id": f"docker:{c.name}",
                "name": f"🐳 {c.name}",
                "type": "docker",
                "container": c.name,
            })
    except Exception as e:
        logger.exception(e)
        print("docker not available", e)

    return result


async def resolve_target(target_id):
    # 1️⃣ 静态 host
    for t in TERMINAL_TARGETS:
        if t["id"] == target_id:
            return t

    # 2️⃣ docker
    if target_id.startswith("docker:"):
        name = target_id.split(":", 1)[1]
        return {
            "type": "docker",
            "container": name
        }

    return None


async def handle_pty(ws: WebSocket):
    pid, fd = pty.fork()

    if pid == 0:
        # 子进程：启动 shell
        os.execvp("bash", ["bash"])

    else:
        # 👉 设置非阻塞（关键！！）
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        async def read():
            try:
                while True:
                    await asyncio.sleep(0.01)  # 👉 防止CPU爆炸

                    try:
                        data = os.read(fd, 1024)
                        if data:
                            await ws.send_text(data.decode(errors="ignore"))
                    except BlockingIOError:
                        continue
                    except OSError:
                        break
            except Exception as e:
                print("read error:", e)

        async def write():
            try:
                while True:
                    msg = await ws.receive_text()

                    # 👉 resize 支持
                    if msg.startswith("{"):
                        try:
                            data = json.loads(msg)
                            if data.get("type") == "resize":
                                import termios, struct
                                fcntl.ioctl(
                                    fd,
                                    termios.TIOCSWINSZ,
                                    struct.pack("HHHH", data["rows"], data["cols"], 0, 0)
                                )
                                continue
                        except:
                            pass

                    os.write(fd, msg.encode())

            except WebSocketDisconnect:
                print("client disconnected")
            except Exception as e:
                print("write error:", e)

        await asyncio.gather(read(), write())

        # 👉 清理
        try:
            os.close(fd)
        except:
            pass


async def handle_ssh(ws, target):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(
        hostname=target["host"],
        username=target["username"],
        password=target["password"],
    )

    channel = client.invoke_shell()

    async def read():
        while True:
            if channel.recv_ready():
                data = channel.recv(1024)
                await ws.send_text(data.decode(errors="ignore"))
            await asyncio.sleep(0.01)

    async def write():
        while True:
            msg = await ws.receive_text()

            # resize（简单版）
            if msg.startswith("{"):
                continue

            channel.send(msg)

    await asyncio.gather(read(), write())


async def handle_host(ws, target):
    mode = target.get("mode", "pty")

    if mode == "ssh":
        await handle_ssh(ws, target)
    else:
        await handle_pty(ws)


@app.websocket("/api/ws/terminal")
async def terminal(ws: WebSocket):
    logger.info(f"received create terminal websocket request, ws: {ws}")
    
    origin = ws.headers.get("origin")
    print(origin)

    await ws.accept()  # 👈 只在这里调用一次

# 🔥 接收初始化参数
    init_msg = await ws.receive_text()
    data = json.loads(init_msg)

    target = data.get("target")
    print(target)

    # target = ws.query_params.get("target")
    # target_id = ws.query_params.get("target")
    # target = await resolve_target(target_id)
    if target and target.get("id").startswith("docker:"):
        name = target.id.split(":", 1)[1]
        target = {
            "type": "docker",
            "container": name
        }

    if target and (target.get("type") == "local" or target.get("type") == "host"):
        await handle_host(ws, target)
    elif target and target.get("type") == "docker":
        await handle_docker(ws, target)
    else:
        await ws.send_text("invalid target")
        await ws.close()


# ===== Jupyter APIs
# 全局保存 Jupyter 子进程
jupyter_process = None
JUPYTER_PORT = 8888
JUPYTER_NOTEBOOK_DIR = "/.notebooks" if is_running_in_docker() else "./.notebooks"


def is_port_ready(port: int, timeout=30, interval=1):
    start = time.time()
    while time.time() - start < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5)
        try:
            sock.connect(("127.0.0.1", port))
            sock.close()
            return True
        except:
            sock.close()
            time.sleep(interval)
    return False


# 启动 JupyterLab
@app.get("/api/jupyter/start-jupyter")
def start_jupyter(request: Request):
    logger.info("received start jupyter labl request")

    global jupyter_process
    if jupyter_process is not None and jupyter_process.poll() is None:
        server_ip = get_current_server_ip(request)
        return {"status": "running", "process_id": f"{jupyter_process}", "url": f"http://{server_ip}:{JUPYTER_PORT}/lab"}

    tornado_settings = '{"headers":{"Content-Security-Policy":"frame-ancestors *"}}' 
    # 后台启动 jupyter（无浏览器、允许远程、关闭token）
    jupyter_process = subprocess.Popen([
        "jupyter", "lab",
        f"--notebook-dir={JUPYTER_NOTEBOOK_DIR}",
        f"--port={JUPYTER_PORT}",
        "--no-browser",
        "--ip=0.0.0.0",
        "--ServerApp.token=''",
        "--ServerApp.allow_origin='*'",  
        "--ServerApp.allow_credentials=True",
        "--ServerApp.allow_remote_access=True",
        "--ServerApp.tornado_settings=" + tornado_settings,
        "--ServerApp.disable_check_xsrf=True"
    ])

    logger.info(f"jupyter lab is starting, waiting for the port startup, process: {jupyter_process.pid}, url: http://localhost:{JUPYTER_PORT}/lab")
    
    # ✅【关键】等待端口真正启动成功，再返回！
    if not is_port_ready(JUPYTER_PORT):
        raise Exception("Jupyter 启动超时")
    
    logger.info(f"jupyter lab is running, process: {jupyter_process.pid}, url: http://localhost:{JUPYTER_PORT}/lab")
    return {"status": "running", "process_id": f"{jupyter_process.pid}", "url": f"http://localhost:{JUPYTER_PORT}/lab"}


# 停止 JupyterLab
@app.get("/api/jupyter/stop-jupyter")
def stop_jupyter():
    logger.info("received stop jupyter lab request")

    global jupyter_process

    try:
        # 1. 先判断进程是否存在
        if jupyter_process is not None:
            pid = jupyter_process.pid
            logger.info(f"🔴 准备杀死 Jupyter 进程：{pid}")

            # 2. 杀死整个进程树（最关键！能杀干净子进程）
            try:
                parent = psutil.Process(pid)
                for child in parent.children(recursive=True):
                    child.kill()  # 杀子进程
                parent.kill()      # 杀主进程
                logger.info(f"✅ 成功杀死 Jupyter 进程树：{pid}")
            except Exception as e:
                logger.warning(f"⚠️ 进程可能已退出：{e}")

            jupyter_process = None
            return {"status": "stopped", "process_id": pid}

        return {"status": "not_running"}
    
    except Exception as e:
        logger.error(f"❌ 停止 Jupyter 失败：{str(e)}")
        return {"status": "error", "msg": str(e)}


# 服务退出时自动杀死 Jupyter
@atexit.register
def kill_jupyter_on_exit():
    stop_jupyter()


# ===== MISC APIs
@app.get("/api/debug/routes")
def routes():
    return [r.path for r in app.routes]


@app.get("/api/whoami")
def whoami():
    logger.info("received get whoami request")
    return {
        "pid": os.getpid(),
        "host": socket.gethostname()
    }


def get_current_server_ip(request: Request) -> str:
    """
    多网卡环境 100% 正确
    返回：当前处理该请求的 本机真实IP
    """
    try:
        # 核心：从 request 底层 socket 拿当前绑定的IP
        return request.scope["server"][0]
    except:
        return "127.0.0.1"


@app.get("/api/api_service/ip")
async def my_api(request: Request):
    logger.info(f"received get api service ip request {request}")
    # 🔥 这就是你当前服务的IP（多网卡也绝对正确）
    server_ip = get_current_server_ip(request)
    
    return {
        "msg": "ok",
        "server_ip": server_ip  # ✅ 绝对正确
    }


@app.get("/jobs/{job_id}/logs/stream")
async def log_stream(job_id: str, request: Request):
    async def event_generator():
        try:
            while True:
                # 客户端断开时退出
                if await request.is_disconnected():
                    break

                try:
                    # 等待日志（带超时，用来发心跳）
                    log = await asyncio.wait_for(
                        log_queues[job_id].get(),
                        timeout=15
                    )

                    yield f"data: {json.dumps(log)}\n\n"

                except asyncio.TimeoutError:
                    # 心跳（防止连接被代理断掉）
                    yield "event: ping\ndata: {}\n\n"

        except asyncio.CancelledError:
            # 客户端断开会触发
            pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )