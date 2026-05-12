from utils.common import is_running_in_docker
from datetime import datetime
import os

# 有环境变量就用，没有就用默认路径，绝不报错
DEEPHOLE_STOCK_DB_PATH = os.environ.get("DEEPHOLE_STOCK_DB_PATH", "/data" if is_running_in_docker() else "./data")
BAOSTOCK_HIS_DB_PATH = os.environ.get("BAOSTOCK_HIS_DB_PATH", "/data" if is_running_in_docker() else "./data")

DB = DEEPHOLE_STOCK_DB_PATH + "/stock.duckdb"


def safe_time(v: str):
    """
    判断 v 的类型：
    - 如果是 datetime 对象 → 保留
    - 如果是 字符串/None/其他 → 转成 None 存入数据库
    """
    if isinstance(v, datetime):
        return v  # 是日期时间，正常返回
    return v if len(v) > 0 else None  # 不是 → 转 None，DuckDB 存为 NULL