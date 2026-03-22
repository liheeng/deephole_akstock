from utils.common import is_running_in_docker
from datetime import datetime

data_volume = "/data" if is_running_in_docker() else "./data"
DB = data_volume + "/stock.duckdb"

def safe_time(v: str):
    """
    判断 v 的类型：
    - 如果是 datetime 对象 → 保留
    - 如果是 字符串/None/其他 → 转成 None 存入数据库
    """
    if isinstance(v, datetime):
        return v  # 是日期时间，正常返回
    return v if len(v) > 0 else None  # 不是 → 转 None，DuckDB 存为 NULL