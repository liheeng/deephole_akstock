from datetime import datetime, timedelta
import baostock as bs


# ====================== 自动获取有效交易日 ======================
def get_recent_trade_day(_date=datetime.now().strftime("%Y-%m-%d")):
    today = datetime.now()
    for i in range(0, 30):
        day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_trade_dates(start_date=day, end_date=day)
        df = rs.get_data()
        if not df.empty and df.iloc[0]["is_trading_day"] == "1":
            return day
    return _date
