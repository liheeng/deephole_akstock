import duckdb
from duckdb import DuckDBPyConnection
import threading
import queue
from typing import Optional, Callable, Any
import pandas as pd


# ==============================
# 底层线程安全 DuckDB 控制器
# ==============================
class DuckDBController:
    _instance = None
    _lock = threading.Lock()
    db_path: str

    def __new__(cls, db_path: str | None = None, read_only=False):
        if cls._instance is None:
            if not db_path:
                raise ValueError(f"Missing argument db_path!")
            cls._instance = super().__new__(cls)
            cls._instance.db_path = db_path
        return cls._instance

    def _get_connection(self, read_only=False):
        return duckdb.connect(self.db_path, read_only=read_only)

    def write(
        self,
        df: Optional[pd.DataFrame] = None,
        sql: Optional[str] = None,
        view_name: str = "temp_df",
        table_name: Optional[str] = None,
        if_exists: str = "append",
        conn: Optional[DuckDBPyConnection] = None
    ):
        with self._lock:
            con = conn or self._get_connection()
            try:
                if df is not None:
                    con.register(view_name, df)

                if if_exists == "replace":
                    con.execute(f"DROP TABLE IF EXISTS {table_name}")
                    if sql:
                        return con.execute(sql)
                    elif table_name:
                        return con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}")
                    else:
                        raise ValueError("必须提供 table_name 或 sql 来创建表")
                else:
                    if sql:
                        return con.execute(sql)
                    elif table_name:
                        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {view_name} WHERE 1=0")
                        con.execute(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
                        return con
                    else:
                        raise ValueError("必须提供 table_name 或 sql 来创建表")
            finally:
                if conn is None:
                    con.close()

    def read(
        self,
        sql: str,
        params: Optional[list] = None,
        fetch_mode: Optional[str] = None,
        callback: Optional[Callable] = None,
        conn: Optional[DuckDBPyConnection] = None
    ) -> Any:
        """
        增强版读取：支持 fetch_mode + callback
        fetch_mode: None / 'one' / 'all' / 'df'
        """
        con = conn or self._get_connection(read_only=True)
        try:
            result = con.execute(sql, params)
            if result:
                if fetch_mode == "one":
                    data = result.fetchone()
                elif fetch_mode == "all":
                    data = result.fetchall()
                elif fetch_mode == "df":
                    data = result.df()
                else:
                    data = result
            else:
                data = result

            if result and callback:
                return callback(data)
            return data
        finally:
            if conn is None:
                con.close()

    def execute(
        self,
        sql: str,
        params: Optional[list] = None,
        fetch_mode: Optional[str] = None,
        callback: Optional[Callable] = None,
        conn: Optional[DuckDBPyConnection] = None
    ):
        """带锁执行任意 SQL（写操作专用）"""
        with self._lock:
            con = conn or self._get_connection()
            try:
                result = con.execute(sql, params)
                if result:
                    if fetch_mode == "one":
                        res = result.fetchone()
                    elif fetch_mode == "all":
                        res = result.fetchall()
                    elif fetch_mode == "df":
                        res = result.df()
                    else:
                        res = result
                else:
                    res = result

                if result and callback:
                    return callback(res)
                return res
            finally:
                if conn is None:
                    con.close()

    def _start_transaction(self) -> DuckDBPyConnection:
        con = self._get_connection()
        con.execute("BEGIN TRANSACTION")
        return con

    def _commit_transaction(self, con: DuckDBPyConnection):
        con.execute("COMMIT TRANSACTION")
        con.close()

    def _rollback_transaction(self, con: DuckDBPyConnection):
        # 执行回滚，放弃所有未提交的修改
        con.execute("ROLLBACK TRANSACTION")
        # 回滚后也必须关闭连接（和 commit 保持一致）
        con.close()


class DuckDBTransaction:
    def __init__(self, db_controller: DuckDBController):
        self.db_controller = db_controller
        self.conn = None

    def __enter__(self):
        # 开启事务
        self.conn = self.db_controller._start_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 有异常 → 回滚
        if exc_type:
            self.db_controller._rollback_transaction(self.conn)
        else:
            self.db_controller._commit_transaction(self.conn)

        # 一定要关闭连接
        self.conn.close()

        # False = 不吞异常（推荐）
        return False

    # =========================
    # DB 操作封装
    # =========================
    def read(self, sql: str, params=None, fetch_mode=None, callback=None):
        return self.db_controller.read(sql, params, fetch_mode, callback, self.conn)

    def execute(self, sql: str, params=None, fetch_mode=None, callback=None):
        return self.db_controller.execute(sql, params, fetch_mode, callback, self.conn)

    def write(self, df=None, sql=None, view_name="temp_df", table_name=None, if_exists="append"):
        return self.db_controller.write(df, sql, view_name, table_name, if_exists, self.conn)

# ==============================
# 【最终版】DuckDB Service（读写一体化）
# 写入 = 串行队列
# 读取 = 并行无锁
# 完全暴露 read / execute
# ==============================
class DuckDBService:
    _instance = None
    _init_lock = threading.Lock()
    db: DuckDBController
    write_queue: queue.Queue
    stop_event: threading.Event

    def __new__(cls, db_path: str):
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.db = DuckDBController(db_path)
                cls._instance.write_queue = queue.Queue()
                cls._instance.stop_event = threading.Event()
                cls._instance._start_worker_thread()
        return cls._instance

    def _start_worker_thread(self):
        worker_thread = threading.Thread(
            target=self._process_write_queue,
            daemon=True,
            name="DuckDB-Write-Worker"
        )
        worker_thread.start()

    def _process_write_queue(self):
        while not self.stop_event.is_set():
            try:
                task = self.write_queue.get(timeout=0.5)
                self._execute_task(task)
                self.write_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[写入服务] 异常: {str(e)}")

    def _execute_task(self, task: dict):
        task_type = task.get("type")
        callback = task.get("callback")
        result, error = None, None

        try:
            if task_type == "sql":
                sql = task["sql"]
                params = task.get("params")
                fetch_mode = task.get("fetch_mode")
                result = self.db.execute(sql, params, fetch_mode)

            elif task_type == "dataframe":
                df = task["df"]
                table_name = task["table_name"]
                if_exists = task.get("if_exists", "append")
                view_name = task.get("view_name", "temp_df")
                result = self.db.write(df=df, table_name=table_name, if_exists=if_exists, view_name=view_name)

        except Exception as e:
            error = e
            print(f"[任务执行失败] {e}")

        if callback:
            try:
                callback(result, error)
            except Exception:
                pass

    # ==========================
    # 【写入接口：串行安全】
    # ==========================
    def submit_sql(
        self,
        sql: str,
        params: Optional[list] = None,
        fetch_mode: Optional[str] = None,
        callback: Optional[Callable[[Any, Optional[Exception]], None]] = None
    ):
        self.write_queue.put({
            "type": "sql", "sql": sql, "params": params, "fetch_mode": fetch_mode, "callback": callback
        })

    def submit_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        callback: Optional[Callable[[Any, Optional[Exception]], None]] = None
    ):
        self.write_queue.put({
            "type": "dataframe", "df": df, "table_name": table_name, "if_exists": if_exists, "callback": callback
        })

    # ==========================
    # 【读取接口：并行高性能】
    # ==========================
    def read(
        self,
        sql: str,
        params: Optional[list] = None,
        fetch_mode: Optional[str] = None,
        callback: Optional[Callable] = None
    ) -> Any:
        """
        增强读接口：
        fetch_mode: None / 'one' / 'all' / 'df'
        """
        return self.db.read(sql, params, fetch_mode, callback)

    # ==========================
    # 【直接执行 SQL（同步带锁）】
    # ==========================
    def execute(
        self,
        sql: str,
        params: Optional[list] = None,
        fetch_mode: Optional[str] = None,
        callback: Optional[Callable] = None
    ):
        """直接暴露底层带锁 execute，支持立即执行写操作"""
        return self.db.execute(sql, params, fetch_mode, callback)

    # ==========================
    # 事务
    # ==========================
    def start_transaction(self):
        return self.db._start_transaction()

    def commit_transaction(self, con):
        self.db._commit_transaction(con)

    # ==========================
    # 关闭服务
    # ==========================
    def stop(self):
        self.stop_event.set()
        self.write_queue.join()


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