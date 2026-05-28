import baostock as bs
import duckdb
import time
import random
from loguru import logger
from tqdm import tqdm
from datetime import datetime

from tools.baostock.append_download import (
    DB_PATH,
    START_DATE as START_DATE_DAY,
    SLEEP_MIN,
    SLEEP_MAX,
    init_database as init_day_db,
    is_today_trading_day,
    get_last_download_dates,
    download_daily,
    download_factor,
    get_recent_trade_day,
)
from tools.baostock.append_5m_download import (
    START_DATE as START_DATE_5M,
    init_database as init_5m_db,
    get_last_download_date,
    is_stock_code,
    download_5m,
)

# 日志
logger.add("./logs/baostock_download.log", rotation="100 MB", encoding="utf-8", enqueue=True)


# ====================== 数据库初始化（合并两套表） ======================
def init_database():
    """初始化所有表：日K、复权因子、5分钟K线"""
    init_day_db()
    init_5m_db()


# ====================== 核心处理（每个股票依次下载日线 + 复权因子 + 5分钟线） ======================
def handle_download(code, start_day, start_5m, end, con):
    # -------- 1. 日K数据 --------
    kline_max, factor_max = get_last_download_dates(con, code)
    download_daily(code, start_day, end, con, kline_max)

    # -------- 2. 复权因子 --------
    download_factor(code, start_day, end, con, factor_max)

    # -------- 3. 5分钟K线（仅股票代码有分钟线） --------
    if is_stock_code(code):
        last_date = get_last_download_date(con, code)
        download_5m(code, start_5m, end, con, last_date)
    else:
        logger.info(f"⏭️ {code} | 指数代码，跳过5分钟K线")


# ====================== 主程序 ======================
def main():
    init_database()
    bs.login()
    if not is_today_trading_day():
        logger.info("⏭️ 今日无交易日，跳过")
        return

    logger.info("✅ 今日交易日，开始下载日线 + 复权因子 + 5分钟K线")

    # 获取最近交易日
    last_trade_date = get_recent_trade_day(datetime.now().strftime("%Y-%m-%d"))

    # 获取股票列表
    stock_df = bs.query_all_stock(day=last_trade_date).get_data()
    codes = stock_df["code"].tolist()
    logger.info(f"📈 总证券数：{len(codes)}")

    con = duckdb.connect(DB_PATH)
    for code in tqdm(codes, desc="下载进度"):
        try:
            handle_download(code, START_DATE_DAY, START_DATE_5M, last_trade_date, con)
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        except Exception as e:
            logger.error(f"❌ {code} | 失败：{str(e)}")

    con.close()
    bs.logout()
    logger.info("🎉 全部完成！")


if __name__ == "__main__":
    main()
