# tests/conftest.py
import sys
from pathlib import Path

# 把 app 目录加入 PYTHONPATH，这是你真正的源码根目录
root_dir = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(root_dir))

from db.db_common import DB
from db.duckdb import DuckDBController
from db.duckdb_tool import execute_batch_sql

if __name__ == "__main__":

    duckdb_controller = DuckDBController(db_path=DB)

    conn = duckdb_controller._get_connection()
    sql = "select distinct symbol from stock_daily where market='CN' group by symbol"
    df = conn.execute(sql).df()
    symbols = df['symbol'].tolist()
    params = []
    for symbol in symbols:
        params.append([symbol])
    sql = "select symbol, market, date from stock_daily where symbol=? order by date desc limit 1"
    df = execute_batch_sql(conn, sql, params)
    print(df)
    conn.close()
