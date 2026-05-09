import baostock as bs
import duckdb
import pandas as pd
import time
import random
from loguru import logger
from tqdm import tqdm
from datetime import datetime, timedelta, date
from utils.trading_uitl import get_target_sync_date

# ====================== 核心配置 ======================
DB_PATH = "./data/baostock_data.duckdb"
START_DATE = "2000-01-01"
# END_DATE = "2026-04-24"
SLEEP_MIN = 1.5
SLEEP_MAX = 3

# 日志
logger.add("./logs/baostock_download.log", rotation="100 MB", encoding="utf-8", enqueue=True)


# ====================== 数据库初始化 ======================
def init_database():
    con = duckdb.connect(DB_PATH)
    # 日K表
    con.execute("""
        CREATE TABLE IF NOT EXISTS kline_day (
            code STRING, date STRING, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            preclose DOUBLE, volume DOUBLE, amount DOUBLE, adjustflag STRING,
            turn DOUBLE, tradestatus STRING, pctChg DOUBLE, peTTM DOUBLE,
            pbMRQ DOUBLE, psTTM DOUBLE, pcfNcfTTM DOUBLE,
            PRIMARY KEY (code, date)
        )
    """)
    # 复权因子表
    con.execute("""
        CREATE TABLE IF NOT EXISTS adjust_factor (
            code STRING, dividOperateDate STRING,
            foreAdjustFactor DOUBLE, backAdjustFactor DOUBLE, adjustFactor DOUBLE,
            PRIMARY KEY (code, dividOperateDate)
        )
    """)
    con.close()


def is_today_trading_day():
    # 今天日期字符串，格式 YYYY-MM-DD
    _end_day = get_target_sync_date()
    
    # 登录（不用注册）
    # 查询单天交易日历
    rs = bs.query_trade_dates(start_date=_end_day, end_date=_end_day)
    if rs.error_code != '0':
        print("query_trade_dates 失败：", rs.error_msg)
        bs.logout()
        return False

    df = rs.get_data()
    # is_trading_day 是 '1' 表示交易日
    if not df.empty and df.iloc[0]['is_trading_day'] == '1':
        return True
    else:
        return False


def get_last_download_dates(con, code):
    # kline 最大日期
    kline_max = con.execute(
        "SELECT MAX(date) FROM kline_day WHERE code = ?",
        [code]
    ).fetchone()[0]

    # 复权因子最大日期
    factor_max = con.execute(
        "SELECT MAX(dividOperateDate) FROM adjust_factor WHERE code = ?",
        [code]
    ).fetchone()[0]

    return kline_max, factor_max


def download_factor(code, start, end, con, factor_max=None):
    try:
        # ✅ 如果已有数据
        if factor_max:
            if factor_max >= end:
                # ✅ 已经是最新 → 直接跳过（不要重刷！）
                logger.info(f"⏭️ {code} | 复权因子已最新，跳过")
                return False
            else:
                # ❗ 不是最新 → 必须全量更新（不能增量）
                logger.info(f"♻️ {code} | 复权因子全量更新（因子可能变动）")
                con.execute("DELETE FROM adjust_factor WHERE code = ?", [code])

        # ✅ 全量拉取
        rs = bs.query_adjust_factor(code, start, end)
        df = rs.get_data()

        if not df.empty:
            df["code"] = code
            con.register("tmp", df)

            con.execute("""
                INSERT INTO adjust_factor
                (code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor)
                SELECT code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor
                FROM tmp
            """)

            con.unregister("tmp")

            logger.info(f"📊 {code} | 复权因子全量更新: {len(df)}条 | 开始日期: {start} | 结束日期: {end}")

            return True
        else:
            logger.info(f"📊 {code} | 复权因子无数据")
            return False
    except Exception as e:
        logger.error(f"❌ {code} | 复权因子失败：{str(e)}")


def download_daily(code, start, end, con, kline_max=None):
    try:
        kline_start = start

        if kline_max:
            next_day = (
                datetime.strptime(kline_max, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")

            kline_start = next_day

            # ✅ 核心新增判断（推荐 >=）
            if kline_start > end:
                logger.info(f"⏭️ {code} | 日K已最新，跳过 | max={kline_max}")
                return False

        fields = "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM"

        rs = bs.query_history_k_data_plus(
            code,
            fields,
            kline_start,
            end,
            "d",
            "3"
        )

        if rs is not None:
            df = rs.get_data()
        else:
            logger.info(f"⏭️ {code} | 日K无数据，跳过")
            return False

        if df.empty:
            logger.info(f"⏭️ {code} | 日K无数据，跳过")
            return False

        # 数据清洗（保持你原逻辑）
        df = df.replace("", None)

        numeric_cols = [
            "open", "high", "low", "close", "preclose",
            "volume", "amount", "turn", "pctChg",
            "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM"
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["tradestatus"] = df["tradestatus"].replace("", None)
        df["code"] = code

        con.register("tmp", df)

        con.execute("""
            INSERT INTO kline_day
            (code, date, open, high, low, close, preclose,
            volume, amount, adjustflag, turn, tradestatus,
            pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM)
            SELECT code, date, open, high, low, close, preclose,
                volume, amount, adjustflag, turn, tradestatus,
                pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM
            FROM tmp
        """)

        con.unregister("tmp")

        logger.success(f"✅ {code} | 日K增量更新：{len(df)} 条 | 起始={kline_start} | 结束={end}")

        return True

    except Exception as e:
        logger.error(f"❌ {code} | 日K失败：{str(e)}")


# ====================== 核心处理（复权因子优先） ======================
def handle_download(code, start, end, con):
    kline_max, factor_max = get_last_download_dates(con, code)

    # -------- 1. 处理：日K数据（指定列名插入，核心修复！） --------
    if not download_daily(code, start, end, con, kline_max):
        return False

    # -------- 2. 处理：复权因子（严格按列名插入，杜绝顺序错误） --------
    download_factor(code, start, end, con, factor_max)


# ====================== 自动获取有效交易日 ======================
def get_recent_trade_day(_date=None):
    today = datetime.now()
    for i in range(1, 30):
        day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_trade_dates(start_date=day, end_date=day)
        df = rs.get_data()
        if not df.empty and df.iloc[0]["is_trading_day"] == "1":
            return day
    return _date


# ====================== 主程序（反序下载） ======================
def main():
    init_database()
    bs.login()
    if not is_today_trading_day():
        logger.info("⏭️ 今日无交易日，跳过")
        return

    logger.info("✅ 今日交易日，开始下载")

    # 获取最近交易日
    last_trade_date = get_recent_trade_day(datetime.now().strftime("%Y-%m-%d"))

    # 获取股票列表
    stock_df = bs.query_all_stock(day=last_trade_date).get_data()
    # revert stock_df
    # codes = stock_df["code"].tolist()[::-1]
    codes = stock_df["code"].tolist()
    # logger.info(f"📈 总股票数：{len(codes)} | 已反序")
    logger.info(f"📈 总股票数：{len(codes)}")

    con = duckdb.connect(DB_PATH)
    for code in tqdm(codes, desc="下载进度"):
        try:
            # if code.startswith("sh.6") or code.startswith("sz.0"):
            handle_download(code, START_DATE, last_trade_date, con)
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            # else:
            #     continue
        except Exception as e:
            logger.error(f"❌ {code} | 失败：{str(e)}")

    con.close()
    bs.logout()
    logger.info("🎉 全部完成！")


if __name__ == "__main__":
    main()
