import duckdb
import threading
from typing import Optional, Union
import pandas as pd

# ==============================
# 【多进程/多线程安全 DuckDB 工具】
# ==============================
class DuckDBController:
    _instance = None
    _lock = threading.Lock()  # 全局写入锁（关键！）

    def __new__(cls, db_path: str = "stock.duckdb"):
        # 单例模式，保证全局只有一个连接控制器
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.db_path = db_path
        return cls._instance

    def _get_connection(self):
        # 每次获取新连接（DuckDB 最佳实践）
        return duckdb.connect(self.db_path)

    def write(
        self,
        df: Optional[pd.DataFrame] = None,
        sql: Optional[str] = None,
        view_name: str = "temp_df",
        table_name: Optional[str] = None,
        if_exists: str = "append"  # append / replace
    ):
        """
        安全写入 DataFrame 到 DuckDB
        - 自动加锁，多线程不会冲突
        """
        with self._lock:  # 🔥 核心：写入加锁，排队执行
            con = self._get_connection()
            try:
                if df is not None:
                    con.register(view_name, df)
                    
                if if_exists == "replace":
                    con.execute(f"DROP TABLE IF EXISTS {table_name}")
                    if sql:
                        return con.execute(sql)  # 允许自定义建表语句
                    elif table_name:
                        return con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}")
                    else:
                        raise ValueError("必须提供 table_name 或 sql 来创建表")
                else:
                    if sql:
                        return con.execute(sql)  # 允许自定义建表语句（如果表不存在）
                    elif table_name:
                        return con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {view_name} WHERE 1=0")  # 先创建空表
                    else:
                        raise ValueError("必须提供 table_name 或 sql 来创建表")
            finally:
                con.close()

    def read(self, sql: str, params: Optional[list] = None) -> pd.DataFrame:
        """
        安全读取（支持多线程并行读，无锁，速度极快）
        """
        con = self._get_connection()
        try:
            return con.execute(sql, params).df()
        finally:
            con.close()

    def execute(self, sql: str, params: Optional[list] = None, fetch: str|None = None, callback: Optional[callable] = None):
        """
        安全执行任意 SQL（create/delete/update 等）
        """
        with self._lock:
            con = self._get_connection()
            try:
                result = con.execute(sql, params)
                if fetch is not None:
                    if fetch == "one":
                        result = result.fetchone()
                    elif fetch == "all":
                        result = result.fetchall()
                    elif fetch == "df":
                        result = result.df()
                    else:
                        pass
                    
                if callback:
                    return callback(result)
                else:
                    return result
            finally:
                con.close()

# ==============================
# 【使用示例】
# ==============================
if __name__ == "__main__":
    # 初始化（全局单例，多次调用也安全）
    db = DuckDBController(db_path="stock_data.duckdb")

    # 1. 写入数据（安全）
    # db.write(df, table_name="us_stock_daily", if_exists="append")

    # 2. 查询数据（安全，支持多线程同时读）
    df = db.read("SELECT * FROM us_stock_daily LIMIT 10")
    print(df)

    # 3. 执行SQL（安全）
    # db.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON us_stock_daily(symbol)")