from typing import Optional, List, Any
import pandas as pd


def execute_batch_sql(conn, sql_template: str, params_list: Optional[List[List[Any]]] | None):
    total_df: pd.DataFrame | None = None
    if params_list is None:
        return conn.execute(sql_template).df()
    
    i = 0
    for params in params_list:
        # sql = sql_template.replace("?", "{}").format(*params)
        df = conn.execute(sql_template, params).df()
        total_df = df if total_df is None else pd.concat([total_df, df])
        print(i)
        i = i + 1

    return total_df


# if __name__ == "__main__":
#     from db.db_common import DB
#     from db.duckdb import DuckDBController

#     duckdb_controller = DuckDBController(db_path=DB)

#     conn = duckdb_controller._get_connection()
#     sql = "select distinct symbol from stock_daily where market='CN' group by symbol"
#     df = conn.execute(sql).df()
#     symbols = df['symbol'].tolist()

#     sql = "select symbol, market, date where symbol=? order by date desc limit 1"
#     df = execute_batch_sql(conn, sql, symbols)
#     print(df)
#     conn.close()
