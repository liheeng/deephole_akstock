from datetime import datetime, timedelta
import akshare as ak
import pandas as pd
import cn_stock_holidays.data_hk as hkex
import exchange_calendars as xcals


def get_target_sync_date() -> str:
    """
    如果当前时间在下午 17 点（17:00）之前，返回前一天日期。
    如果当前时间在下午 17 点（17:00）及之后，返回当天日期。
    返回格式为 "%Y-%m-%d"
    """
    now = datetime.now()
    if now.hour < 17:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        return now.strftime("%Y-%m-%d")


def is_trading_day(market: str = "CN", date_str: str | None = None) -> bool:
    target_day = datetime.today().strftime("%Y-%m-%d") if not date_str else date_str
    
    if market == "CN":
        cal = ak.tool_trade_date_hist_sina()
        trade_days = set(cal["trade_date"].astype(str))
        return target_day in trade_days
    elif market == "HK":
        d = datetime.strptime(target_day, "%Y-%m-%d").date()
        return hkex.is_trading_day(d)
    elif market == "US":
        xnys = xcals.get_calendar("XNYS")
        return xnys.is_session(target_day)
    else:
        return False


def is_trading_today(market: str = "CN") -> bool:
    # 1. 获取今天的 datetime 对象
    today = datetime.today().date()  # 只保留 年月日

    if market == "CN":
        df = ak.tool_trade_date_hist_sina()
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return today in df["trade_date"].values

    elif market == "HK":
        return hkex.is_trading_day(today)
    elif market == "US":
        xnys = xcals.get_calendar("XNYS")
        return xnys.is_session(today.strftime("%Y-%m-%d"))
    else:
        return False
