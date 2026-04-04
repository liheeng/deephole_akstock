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