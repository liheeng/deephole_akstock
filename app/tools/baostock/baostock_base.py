import time
from datetime import datetime, timedelta
import baostock as bs
from func_timeout.exceptions import FunctionTimedOut
from func_timeout import func_timeout
from loguru import logger


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


def safe_logout(timeout=5):
    """安全的登出，带超时控制"""
    try:
        func_timeout(timeout, bs.logout)
        return True
    except FunctionTimedOut:
        logger.warning("登出操作超时，强制跳过")
        return False
    except Exception as e:
        logger.warning(f"登出异常: {e}")
        return False


def refresh_baostock_connection():
    """刷新 BaoStock 连接（登出+登录）"""
    safe_logout()
    time.sleep(5)   # 给网络一点恢复时间
    lg = bs.login()
    if lg.error_code != '0':
        raise ConnectionError(f"重新登录失败: {lg.error_msg}")
    logger.info("BaoStock 连接已刷新")