# app/api.py

import os
import time
from typing import Optional, List, Any, Dict
from contextlib import asynccontextmanager, closing
from fastapi import FastAPI, HTTPException, WebSocket, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
import subprocess
import asyncio
from pydantic import BaseModel
import pandas as pd
import traceback
import re
import json
from datetime import datetime
import nanoid

from fastapi.middleware.cors import CORSMiddleware
from app.api_config import CORS_CONFIG

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

from backtest.backtest_base import DataSetConfig, BacktestRequest
from vectorbt_test.core.registry import NodeRegistry
from vectorbt_test.core.portfolio import PortfolioParameters, PortfolioResultWrapper
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.engine.portfolio_builder import PortfolioBuilder
from vectorbt_test.engine.init import load_register_nodes
from vectorbt_test.portfolios.signal_strategy_portfolio import StrategyOp
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
    
    load_register_nodes()
    logger.info("load backtest's register nodes")
    # 启动工作线程池
    start_workers(n=4)   # 👈 在这里启动
    logger.info("Worker threads started")


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
        tasks = task_manager.list_tasks(limit)
        logger.info("list tasks: %s", tasks)
        return tasks
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
        

@app.post("/export/preview")
def export_preview(req: dict):
    validate_req(req)
    _req = {**req}
    if _req.get("limit") and int(_req["limit"]) > 50:
        _req["limit"] = 50
    sql = build_sql(_req)
    validate_sql(sql)

    with closing(db_controller._get_connection(read_only=True)) as con:
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


@app.post("/export/stream")
async def export_stream(req):
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


@app.get("/nodes")
def get_nodes():
    nodes = NodeRegistry.to_dict()
    # logger.info(f"nodes: {nodes}")
    logger.info(f"get all nodes: {nodes.keys()}")
    return nodes


def load_data_somehow(ds: DataSetConfig) -> pd.DataFrame:
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
            json.dumps(stats, ensure_ascii=False),
            json.dumps(equity, ensure_ascii=False),
            json.dumps(trades, ensure_ascii=False)
        ])

        return result_id
    except Exception as e:
        logger.exception(f"failed to save backtest result, error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/backtest")
def run_backtest(req: BacktestRequest):
    try:
        # 1. 构建 Portfolio
        portfolio_config = req.portfolio_config
        builder = PortfolioBuilder.new(portfolio_config.name, portfolio_config.mode)

        for s in portfolio_config.strategies:
            builder.add_strategy(s.name)

            for f in s.factors:
                builder.add_factor(f)

            builder.end_strategy()

        builder.set_strategy_op(portfolio_config.strategy_op or StrategyOp.OR.value)

        if portfolio_config.schedule_signal:
            builder.set_schedule_signal(portfolio_config.schedule_signal)

        builder.set_portfolio_params(
            PortfolioParameters(**portfolio_config.params)
        )

        portfolio = builder.build()

        # 2. 加载数据
        df = load_data_somehow(req.dataset_config)

        # 3. 运行回测
        pf = portfolio.run(DataProvider(None), df)
        pfwrapper = PortfolioResultWrapper(pf)

        # detailed_stats = pfwrapper.get_pf_stats(agg_func=None)
        # print(detailed_stats.index.tolist()) # 看看具体的指标名到底叫什么

        equity_curve = pfwrapper.get_pf_value_dict(as_json=False)
        # print(equity_curve)
    
        _stats = pfwrapper.get_pf_stats(as_json=False)
        stats = pfwrapper.clean_for_json(_stats)
        # print(stats)
        trades = pf.trades.records_readable.to_dict(orient="records")
        # ==========================
        # ✅ 保存到数据库
        # ==========================
        equity_curve_dict = json.loads(equity_curve) if isinstance(equity_curve, str) else equity_curve
        save_backtest_result(
            dataset_config_id=req.dataset_config.id,
            portfolio_name=portfolio_config.name,
            stats=stats,
            equity=equity_curve_dict,
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


@app.get("/backtest/results", response_model=List[BacktestResultRow])
def query_backtest_results(
    dataset_config_id: Optional[str] = None,
    portfolio_name: Optional[str] = None
):
    conn = db_controller
    try:
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
# ✅ 接口：新增 dataset
# ==============================
@app.post("/backtest/dataset")
def backtest_create_dataset(req: DatasetCreateRequest):
    conn = db_controller

    try:
        # 先检查 ID 是否重复
        exists = conn.read(
            sql="SELECT 1 FROM datasets WHERE id = ?", params=[req.id], fetch_mode="one")

        if exists:
            raise HTTPException(status_code=400, detail=f"数据集 ID {req.id} 已存在")

        # 插入 SQL
        sql = """
            INSERT INTO datasets (
                id, name, createdAt, sourceDef, schema, rowCount, cache
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        # 参数
        params = [
            req.id,
            req.name,
            datetime.now().isoformat(),  # 自动生成创建时间
            json.dumps(req.sourceDef, ensure_ascii=False),  # JSON 存储
            json.dumps(req.schema, ensure_ascii=False) if req.schema else None,
            req.rowCount,
            json.dumps(req.cache, ensure_ascii=False) if req.cache else None
        ]

        conn.execute(sql, params)

        return {
            "success": True,
            "message": "数据集创建成功",
            "id": req.id
        }

    except Exception as e:
        logger.exception(f"failed to create dataset\n{e}")
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}\n{traceback.format_exc()}")


# ==============================
# ✅ 接口：查询所有 datasets
# ==============================
@app.get("/backtest/datasets", response_model=List[Any])
def backtest_fetch_datasets():
    conn = db_controller
    
    try:
        # 查询所有数据集
        query = """
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
        """
        result = conn.execute(query, fetch_mode="all")
        
        # 列名
        columns = ["id", "name", "createdAt", "sourceDef", "schema", "rowCount", "cache"]
        
        # 转成字典列表
        datasets = []
        if not result:
            return datasets
        
        for row in result:
            item = dict(zip(columns, row))
            
            # JSON 字段解析
            item["sourceDef"] = json.loads(item["sourceDef"]) if item["sourceDef"] else {}
            item["schema"] = json.loads(item["schema"]) if item["schema"] else None
            item["cache"] = json.loads(item["cache"]) if item["cache"] else None
            
            datasets.append(item)
        
        return datasets
    except Exception as e:
        logger.exception(f"failed to fetch datasets\n{e}")
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}\n{traceback.format_exc()}")


# ==============================
# Pydantic 模型（和前端 TS 完全对应）
# ==============================
class BacktestConfig(BaseModel):
    id: str
    name: str
    portfolio_mode: str
    params: Dict[str, Any]
    schedule_signal: Dict[str, Any]
    strategy_op: Dict[str, Any]
    vote_weights: Dict[str, Any]
    strategy_weights: Dict[str, Any]
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ==============================
# 1. 获取所有回测配置
# ==============================
@app.get("/backtest/configs", response_model=List[BacktestConfig])
def get_all_backtest_configs():
    conn = db_controller
    try:
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
            # 解析 JSON 字段
            item["params"] = json.loads(item["params"]) if item["params"] else {}
            item["schedule_signal"] = json.loads(item["schedule_signal"]) if item["schedule_signal"] else {}
            item["strategy_op"] = json.loads(item["strategy_op"]) if item["strategy_op"] else {}
            item["vote_weights"] = json.loads(item["vote_weights"]) if item["vote_weights"] else {}
            item["strategy_weights"] = json.loads(item["strategy_weights"]) if item["strategy_weights"] else {}
            result.append(item)

        return result

    except Exception as e:
        logger.exception(f"failed to fetch backtest configs\n{e}")
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}\n{traceback.format_exc()}")


# ==============================
# 2. 保存（新增）一个 backtest config
# ==============================
@app.post("/backtest/config")
def create_backtest_config(config: BacktestConfig):
    conn = db_controller
    try:
        # 检查 ID 是否重复
        exists = conn.execute(
            "SELECT 1 FROM backtest_config WHERE id = ?", [config.id]
        ).fetchone()

        if exists:
            raise HTTPException(status_code=400, detail=f"backtest id {config.id} 已存在")

        now = datetime.now().isoformat()

        conn.execute("""
            INSERT INTO backtest_config (
                id, name, portfolio_mode, params,
                schedule_signal, strategy_op, vote_weights, strategy_weights,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            config.id,
            config.name,
            config.portfolio_mode,
            json.dumps(config.params),
            json.dumps(config.schedule_signal),
            json.dumps(config.strategy_op),
            json.dumps(config.vote_weights),
            json.dumps(config.strategy_weights),
            now,
            now
        ])

        return {
            "success": True,
            "message": "保存成功",
            "id": config.id
        }

    except Exception as e:
        logger.exception(f"failed to create backtest config\n{e}")
        raise HTTPException(status_code=500, detail=f"错误: {str(e)}\n{traceback.format_exc()}")


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
