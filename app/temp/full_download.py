import baostock as bs
import duckdb
import pandas as pd
import time
import random
from loguru import logger
from tqdm import tqdm
from datetime import datetime, timedelta

# ====================== 核心配置 ======================
DB_PATH = "baostock_data.duckdb"
START_DATE = "2000-01-01"
END_DATE = "2026-04-24"
SLEEP_MIN = 1
SLEEP_MAX = 2.5

# 日志
logger.add("baostock_download.log", rotation="100 MB", encoding="utf-8", enqueue=True)


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


# ====================== 核心处理（复权因子优先） ======================
def process_stock(code, con):
    # -------- 1. 优先处理：复权因子（严格按列名插入，杜绝顺序错误） --------
    try:
        con.execute("DELETE FROM adjust_factor WHERE code = ?", [code])
        rs = bs.query_adjust_factor(code, START_DATE, END_DATE)
        df = rs.get_data()
        if not df.empty:
            df["code"] = code
            con.register("tmp", df)
            # ✅ 修复：指定列名插入，不依赖顺序
            con.execute("""
                INSERT INTO adjust_factor (code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor)
                SELECT code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor FROM tmp
            """)
            con.unregister("tmp")
            logger.info(f"📊 {code} | 复权因子保存成功: {len(df)}条")
        else:
            logger.info(f"📊 {code} | 复权因子：无数据")
    except Exception as e:
        logger.error(f"❌ {code} | 复权因子失败：{str(e)}")

    # -------- 2. 处理：日K数据（指定列名插入，核心修复！） --------
    try:
        con.execute("DELETE FROM kline_day WHERE code = ?", [code])
        fields = "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM"
        rs = bs.query_history_k_data_plus(code, fields, START_DATE, END_DATE, "d", "3")
        df = rs.get_data()
        if df.empty:
            return

        # 清洗空值
        # ✅ 一步到位清洗
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
        # ✅ 终极修复：严格指定列名插入，彻底解决类型转换错误
        con.execute("""
            INSERT INTO kline_day 
            (code, date, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM)
            SELECT code, date, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM FROM tmp
        """)
        con.unregister("tmp")
        logger.success(f"✅ {code} | 日K保存成功：{len(df)} 条")
    except Exception as e:
        logger.error(f"❌ {code} | 日K失败：{str(e)}")


# ====================== 自动获取有效交易日 ======================
def get_recent_trade_day():
    today = datetime.now()
    for i in range(1, 30):
        day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_trade_dates(start_date=day, end_date=day)
        df = rs.get_data()
        if not df.empty and df.iloc[0]["is_trading_day"] == "1":
            return day
    return "2026-04-24"


# ====================== 主程序（反序下载） ======================
def main():
    init_database()
    bs.login()
    END_DATE = get_recent_trade_day()
    # 获取股票列表 + 反序（你的要求）
    stock_df = bs.query_all_stock(day=END_DATE).get_data()
    # codes = stock_df["code"].tolist()[::-1]
    codes = stock_df["code"].tolist()
    # logger.info(f"📈 总股票数：{len(codes)} | 已反序")
    logger.info(f"📈 总股票数：{len(codes)}")

    con = duckdb.connect(DB_PATH)
    for code in tqdm(codes, desc="下载进度"):
        # if code.startswith("sh.6") or code.startswith("sz.0"):
        process_stock(code, con)
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        # else:
        #     continue

    con.close()
    bs.logout()
    logger.info("🎉 全部完成！")

if __name__ == "__main__":
    main()