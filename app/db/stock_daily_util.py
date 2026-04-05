from markets.market import Region
from datetime import datetime
import pandas as pd

def get_symbols(db, region: Region) -> list[str]:
    sql = f"SELECT DISTINCT symbol FROM stock_daily WHERE market='{region.value.upper()}'"
    result = db.read(sql, fetch_mode='all')
    return result


def get_symbol_data(db, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    start = start_date if start_date else "1990-01-01"
    end = end_date if end_date else datetime.now().strftime("%Y-%m-%d")
    sql = f"SELECT * FROM stock_daily WHERE symbol='{symbol}' and date>='{start}' and date<='{end}' ORDER BY date"
    return db.read(sql, fetch_mode='df')

def get_last_date(db, symbol: str) -> datetime | None:
    r = db.execute(
        "SELECT max(date) FROM stock_daily WHERE symbol=?", 
        [symbol],
        fetch_mode="one")
    return r[0] if r else None


def get_last_dates(db, symbols_str: str) -> dict[str, datetime | None]:
    # 切割并清洗股票代码
    symbols = [s.strip() for s in symbols_str.split(",") if s.strip()]
    
    # 关键：给每个 symbol 加上单引号！
    quoted_symbols = [f"'{sym}'" for sym in symbols]
    in_clause = ",".join(quoted_symbols)  # 变成 '000001','000002'

    # 查询
    rows = db.execute(
        f"SELECT symbol, MAX(date) FROM stock_daily WHERE symbol IN ({in_clause}) GROUP BY symbol",
        fetch_mode="all"
    )

    # 构建字典（没有数据返回 None）
    result = {s: None for s in symbols}
    for row in rows:
        result[row[0]] = row[1]

    return result

