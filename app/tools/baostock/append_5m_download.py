import baostock as bs
import duckdb
import pandas as pd
import time
import random
from loguru import logger
from tqdm import tqdm
from datetime import datetime, timedelta
from utils.trading_uitl import get_target_sync_date
import os
from utils.common import is_running_in_docker
from func_timeout import func_set_timeout
from func_timeout.exceptions import FunctionTimedOut
from tools.baostock.baostock_base import get_recent_trade_day

# ====================== 核心配置 ======================
BAOSTOCK_HIS_DB_PATH = os.environ.get("BAOSTOCK_HIS_DB_PATH", "/data" if is_running_in_docker() else "./data")
DB_PATH = BAOSTOCK_HIS_DB_PATH + "/baostock_data.duckdb"
START_DATE = "2026-01-01"
SLEEP_MIN = 1.5
SLEEP_MAX = 3

# 日志
logger.add("./logs/baostock_5m_download.log", rotation="100 MB", encoding="utf-8", enqueue=True)


# ====================== 数据库初始化 ======================
def init_database():
    con = duckdb.connect(DB_PATH)
    # 5分钟K线表
    con.execute("""
        CREATE TABLE IF NOT EXISTS kline_5minutes (
            code STRING, date STRING, time STRING,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, adjustflag STRING,
            amplitude_pct DOUBLE,
            amount_log DOUBLE,
            activity_bias DOUBLE,
            PRIMARY KEY (code, date, time)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_kline_5m_code_date_time ON kline_5minutes(code, date, time);
    """)

    # 兼容旧表：新增列（已存在则忽略）
    for col in ["amplitude_pct", "amount_log", "activity_bias"]:
        try:
            con.execute(f"ALTER TABLE kline_5minutes ADD COLUMN {col} DOUBLE;")
        except Exception:
            pass
    con.close()


def is_today_trading_day():
    _end_day = get_target_sync_date()

    rs = bs.query_trade_dates(start_date=_end_day, end_date=_end_day)
    if rs.error_code != '0':
        print("query_trade_dates 失败：", rs.error_msg)
        bs.logout()
        return False

    df = rs.get_data()
    if not df.empty and df.iloc[0]['is_trading_day'] == '1':
        return True
    else:
        return False


def get_last_download_date(con, code):
    """获取该股票5分钟K线已下载的最大日期"""
    try:
        result = con.execute(
            "SELECT date FROM kline_5minutes WHERE code = ? ORDER BY date DESC LIMIT 1;",
            [code]
        ).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def is_stock_code(code):
    """判断是否为股票代码（排除指数代码），指数没有分钟线"""
    # 上海指数: sh.000xxx ~ sh.009xxx
    # 深圳指数: sz.399xxx
    if code.startswith("sh.00"):
        return False
    if code.startswith("sz.399"):
        return False
    return True


# 10 秒强制超时
@func_set_timeout(10)
def safe_download(code, start, end):
    fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
    try:
        rs = bs.query_history_k_data_plus(
            code,
            fields,
            start,
            end,
            frequency="5",
            adjustflag="2"   # 复权状态(1：后复权， 2：前复权，3：不复权)
        )

        if rs is not None:
            return rs.get_data()
        else:
            logger.info(f"⏭️ {code} | 5分钟K线无数据，跳过")
            return None

    except Exception as e:
        # 其他错误 → 直接跳过
        logger.exception(f"❌ {code} 下载失败: {str(e)}")
        return None


def download_5m(code, start, end, con, last_date=None):
    try:
        kline_start = start

        if last_date:
            next_day = (
                datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")

            kline_start = next_day

            if kline_start > end:
                logger.info(f"⏭️ {code} | 5分钟K线已最新，跳过 | max={last_date}")
                return False

        try:
            df = safe_download(code, kline_start, end)  # type: ignore[call-arg]
        except FunctionTimedOut as e:
            logger.error(f"⏰ {code} | 5分钟K线超时，刷新 baostock 连接后重试...{str(e)}")
            bs.logout()
            time.sleep(5)
            bs.login()
            logger.info(f"🔄 {code} | 重试下载5分钟K线...")
            try:
                df = safe_download(code, kline_start, end)  # type: ignore[call-arg]
            except (FunctionTimedOut, Exception) as e2:
                logger.exception(f"❌ {code} | 5分钟K线重试仍然失败：{str(e2)}")
                return False
        except Exception as e:
            logger.exception(f"❌ {code} | 5分钟K线下载失败：{str(e)}")
            return False

        if df is None or df.empty:
            logger.info(f"⏭️ {code} | 5分钟K线无数据，跳过")
            return False

        # 数据清洗
        df = df.replace("", None)

        numeric_cols = ["open", "high", "low", "close", "volume", "amount"]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["code"] = code

        con.register("tmp", df)

        con.execute("""
            INSERT INTO kline_5minutes
            (code, date, time, open, high, low, close, volume, amount, adjustflag,
             amplitude_pct, amount_log, activity_bias)
            SELECT code, date, time, open, high, low, close, volume, amount, adjustflag,
                   ((high - low) / NULLIF(open, 0) * 100),
                   LN(NULLIF(amount, 0) + 1),
                   (close - open) / NULLIF(high - low, 0)
            FROM tmp
        """)

        con.unregister("tmp")

        logger.success(f"✅ {code} | 5分钟K线增量更新：{len(df)} 条 | 起始={kline_start} | 结束={end}")

        return True

    except (FunctionTimedOut, Exception) as e:
        logger.exception(f"❌ {code} | 5分钟K线失败：{str(e)}")
        return False


# ====================== 核心处理 ======================
def handle_download(code, start, end, con):
    last_date = get_last_download_date(con, code)
    download_5m(code, start, end, con, last_date)


# ====================== 主程序 ======================
def main():
    init_database()
    bs.login()
    if not is_today_trading_day():
        logger.info("⏭️ 今日无交易日，跳过")
        return

    logger.info("✅ 今日交易日，开始下载5分钟K线")

    # 获取最近交易日
    last_trade_date = get_recent_trade_day(datetime.now().strftime("%Y-%m-%d"))

    # 获取股票列表，过滤指数代码（指数没有分钟线）
    stock_df = bs.query_all_stock(day=last_trade_date).get_data()
    codes = [c for c in stock_df["code"].tolist() if is_stock_code(c)]
    logger.info(f"📈 总股票数：{len(stock_df)} | 过滤指数后：{len(codes)}")

    counter = 0
    reset_interval = 50  # 每下载50只股票，强制重连 baostock

    con = duckdb.connect(DB_PATH)
    for code in tqdm(codes, desc="下载5分钟K线进度"):
        try:
            # ========== 核心：每 N 次 重连 baostock ==========
            counter += 1
            if counter % reset_interval == 0:
                logger.info("=== 刷新 baostock 连接，防止阻塞 ===")
                bs.logout()
                time.sleep(5)
                bs.login()

            handle_download(code, START_DATE, last_trade_date, con)

            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        except Exception as e:
            logger.exception(f"❌ {code} | 失败：{str(e)}")

    con.close()
    bs.logout()
    logger.info("🎉 全部完成！")


if __name__ == "__main__":
    main()
