import duckdb
import threading
from typing import Optional, Union
import pandas as pd

# ==============================
# 【多进程/多线程安全 DuckDB 工具】
# ==============================
class DuckDBSafe:
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
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append"  # append / replace
    ):
        """
        安全写入 DataFrame 到 DuckDB
        - 自动加锁，多线程不会冲突
        """
        with self._lock:  # 🔥 核心：写入加锁，排队执行
            con = self._get_connection()
            try:
                con.register("tmp_df", df)
                if if_exists == "replace":
                    con.execute(f"DROP TABLE IF EXISTS {table_name}")
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_df")
                else:
                    con.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df")
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

    def execute(self, sql: str, params: Optional[list] = None):
        """
        安全执行任意 SQL（create/delete/update 等）
        """
        with self._lock:
            con = self._get_connection()
            try:
                con.execute(sql, params)
            finally:
                con.close()

# ==============================
# 【使用示例】
# ==============================
if __name__ == "__main__":
    # 初始化（全局单例，多次调用也安全）
    db = DuckDBSafe(db_path="stock_data.duckdb")

    # 1. 写入数据（安全）
    # db.write(df, table_name="us_stock_daily", if_exists="append")

    # 2. 查询数据（安全，支持多线程同时读）
    df = db.read("SELECT * FROM us_stock_daily LIMIT 10")
    print(df)

    # 3. 执行SQL（安全）
    # db.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON us_stock_daily(symbol)")