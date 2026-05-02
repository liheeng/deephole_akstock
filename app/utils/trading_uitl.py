from datetime import datetime
import akshare as ak


def is_trading_day(market: str = "CN", date_str: str | None = None) -> bool:
    today = datetime.today().strftime("%Y-%m-%d") if not date_str else date_str
    
    if market == "CN":
        cal = ak.tool_trade_date_hist_sina()
    elif market == "HK":
        cal = ak.hk_tradecal()
    elif market == "US":
        cal = ak.us_tradecal()
    else:
        return False
    
    trade_days = set(cal["trade_date"].astype(str))
    return today in trade_days


def is_trading_today(market: str = "CN") -> bool:
    # 1. 获取今天的 datetime 对象
    today = datetime.today().date()  # 只保留 年月日

    # 2. 获取对应市场日历
    if market == "CN":
        df = ak.tool_trade_date_hist_sina()
    elif market == "HK":
        df = ak.hk_tradecal()
    elif market == "US":
        df = ak.us_tradecal()
    else:
        return False

    # 3. 【关键】把 akshare 的日期转成 datetime.date
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # 4. 直接判断（纯 datetime 对比，不会有格式问题）
    return today in df["trade_date"].values