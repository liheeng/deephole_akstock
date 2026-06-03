"""
根据 kline_day 表的日K数据，增量计算技术指标并写入 bs_indicators 表。

指标定义与 stock_indicators 完全一致：
  MA5/10/20/60/120, EMA12/26, MACD, RSI14, ATR14,
  KDJ, Bollinger, Vol_MA5/10/20, OBV, 收益率, 价格偏离度

用法:
  python update_baostock_indicators.py
"""

import duckdb
import gc
import os
import numpy as np
import pandas as pd
from loguru import logger
from utils.common import is_running_in_docker

BAOSTOCK_HIS_DB_PATH = os.environ.get(
    "BAOSTOCK_HIS_DB_PATH", "/data" if is_running_in_docker() else "./data"
)
DB_PATH = BAOSTOCK_HIS_DB_PATH + "/baostock_data.duckdb"

logger.add("./logs/baostock_indicators.log", rotation="100 MB", encoding="utf-8", enqueue=True)


# ======================== 表结构 ========================
BS_INDICATORS_DDL = """
    CREATE TABLE IF NOT EXISTS bs_indicators (
        symbol VARCHAR,
        date DATE,

        -- 趋势类
        ma5 DOUBLE, ma10 DOUBLE, ma20 DOUBLE, ma60 DOUBLE, ma120 DOUBLE,
        ema12 DOUBLE, ema26 DOUBLE,

        -- MACD
        macd DOUBLE, macd_signal DOUBLE, macd_hist DOUBLE,

        -- 动量类
        rsi14 DOUBLE,
        k DOUBLE, d DOUBLE, j DOUBLE,

        -- 波动率
        atr14 DOUBLE,
        boll_mid DOUBLE, boll_up DOUBLE, boll_down DOUBLE,

        -- 成交量
        vol_ma5 DOUBLE, vol_ma10 DOUBLE, vol_ma20 DOUBLE,
        obv DOUBLE,

        -- 收益率
        ret_1d DOUBLE, ret_5d DOUBLE, ret_20d DOUBLE,

        -- 价格位置
        pct_from_ma20 DOUBLE,

        -- 系统字段
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_bs_indicators_symbol_date
    ON bs_indicators(symbol, date);
"""


# ======================== 批量计算：EMA/MACD/RSI/KDJ（需 pandas EWM） ========================
# ======================== 批量处理 ========================
BATCH_SIZE = 15  # 每批 15 只（RK3399 4G 内存限制，避免 DuckDB 窗口函数 OOM）


def get_stale_codes(con):
    """获取需要更新的股票代码列表"""
    rows = con.execute("""
        SELECT
            d.code,
            MAX(d.date::DATE) AS daily_max,
            MAX(i.date) AS ind_max
        FROM kline_day d
        LEFT JOIN bs_indicators i ON d.code = i.symbol
        GROUP BY d.code
        HAVING ind_max IS NULL OR MAX(d.date::DATE) > MAX(i.date)
        ORDER BY d.code
    """).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """pandas 全量计算所有技术指标（结果与 update_indicators.py 一致）"""
    df = df.sort_values(["symbol", "date"]).copy()

    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["close", "high", "low"])
    df["volume"] = df["volume"].fillna(0)
    df.loc[df["close"] <= 0, "close"] = np.nan
    df.loc[df["high"] <= 0, "high"] = np.nan
    df.loc[df["low"] <= 0, "low"] = np.nan
    df = df.dropna(subset=["close", "high", "low"])

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    # ---- MA ----
    df["ma5"] = close.rolling(5, min_periods=5).mean()
    df["ma10"] = close.rolling(10, min_periods=10).mean()
    df["ma20"] = close.rolling(20, min_periods=20).mean()
    df["ma60"] = close.rolling(60, min_periods=60).mean()
    df["ma120"] = close.rolling(120, min_periods=120).mean()

    # ---- EMA ----
    df["ema12"] = close.ewm(span=12, adjust=False).mean()
    df["ema26"] = close.ewm(span=26, adjust=False).mean()

    # ---- MACD ----
    df["macd"] = df["ema12"] - df["ema26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    # ---- RSI14 ----
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(14, min_periods=14).mean()
    avg_loss = loss.rolling(14, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))
    df.loc[(avg_loss == 0) & (avg_gain > 0), "rsi14"] = 100
    df.loc[(avg_gain == 0) & (avg_loss > 0), "rsi14"] = 0

    # ---- ATR14 ----
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=14).mean()

    # ---- Bollinger ----
    mid = close.rolling(20, min_periods=20).mean()
    std = close.rolling(20, min_periods=20).std()
    df["boll_mid"] = mid
    df["boll_up"] = mid + 2 * std
    df["boll_down"] = mid - 2 * std

    # ---- KDJ ----
    low_n = low.rolling(9, min_periods=9).min()
    high_n = high.rolling(9, min_periods=9).max()
    denom = high_n - low_n
    rsv = ((close - low_n) / denom.replace(0, np.nan)) * 100
    rsv = rsv.fillna(50)
    df["k"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df["d"] = df["k"].ewm(alpha=1 / 3, adjust=False).mean()
    df["j"] = 3 * df["k"] - 2 * df["d"]

    # ---- Vol MA ----
    df["vol_ma5"] = volume.rolling(5, min_periods=5).mean()
    df["vol_ma10"] = volume.rolling(10, min_periods=10).mean()
    df["vol_ma20"] = volume.rolling(20, min_periods=20).mean()

    # ---- OBV ----
    direction = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    df["obv"] = (direction * volume).fillna(0).cumsum()

    # ---- Returns ----
    _close = close.ffill()
    df["ret_1d"] = _close.pct_change(1)
    df["ret_5d"] = _close.pct_change(5)
    df["ret_20d"] = _close.pct_change(20)

    # ---- Price Position ----
    ma20_safe = df["ma20"].replace(0, np.nan)
    df["pct_from_ma20"] = (close - ma20_safe) / ma20_safe

    # ---- inf -> NULL ----
    df = df.replace([np.inf, -np.inf], np.nan)

    return df


# 每重建一次连接的批次数
RECONNECT_INTERVAL = 30


def get_connection():
    """创建新连接并设置内存上限"""
    con = duckdb.connect(DB_PATH)
    con.execute("SET memory_limit = '2GB'")
    return con


def process_batch(con, codes_batch: list[str]):
    """处理一批股票的指标计算"""
    quoted = [f"'{c}'" for c in codes_batch]
    in_clause = ",".join(quoted)

    # 简单 SQL，无窗口函数，避免 DuckDB 内部 OOM
    df = con.execute(f"""
        SELECT code AS symbol, date::DATE AS date,
               open, high, low, close, volume
        FROM kline_day
        WHERE code IN ({in_clause})
        ORDER BY symbol, date
    """).df()

    if df.empty:
        return 0

    # pandas 全量计算
    df = calculate_indicators(df)

    # 写入
    con.register("tmp_batch", df)
    con.execute("""
        INSERT OR REPLACE INTO bs_indicators (
            symbol, date,
            ma5, ma10, ma20, ma60, ma120,
            ema12, ema26,
            macd, macd_signal, macd_hist,
            rsi14, atr14,
            k, d, j,
            boll_mid, boll_up, boll_down,
            vol_ma5, vol_ma10, vol_ma20,
            obv,
            ret_1d, ret_5d, ret_20d,
            pct_from_ma20,
            create_time, update_time
        )
        SELECT
            symbol, date,
            ma5, ma10, ma20, ma60, ma120,
            ema12, ema26,
            macd, macd_signal, macd_hist,
            rsi14, atr14,
            k, d, j,
            boll_mid, boll_up, boll_down,
            vol_ma5, vol_ma10, vol_ma20,
            obv,
            ret_1d, ret_5d, ret_20d,
            pct_from_ma20,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM tmp_batch
    """)
    con.unregister("tmp_batch")
    n = len(df)
    del df
    return n


# ======================== 主函数 ========================
def main():
    con = duckdb.connect(DB_PATH)

    # 创建 bs_indicators 表
    for stmt in BS_INDICATORS_DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt + ";")

    # 获取需要更新的股票
    stale = get_stale_codes(con)
    logger.info(f"需要更新的股票数: {len(stale)}")

    # 分批处理
    total_inserted = 0
    for i in range(0, len(stale), BATCH_SIZE):
        batch = stale[i:i + BATCH_SIZE]
        codes_batch = [r[0] for r in batch]
        try:
            inserted = process_batch(con, codes_batch)
            total_inserted += inserted
            logger.info(f"✅ 批次 {i//BATCH_SIZE + 1}/{(len(stale)-1)//BATCH_SIZE + 1}: {len(codes_batch)} 只, 更新 {inserted} 行")
        except Exception:
            logger.exception(f"❌ 批次 {i//BATCH_SIZE + 1} 失败，跳过该批")
            continue
        finally:
            # 主动释放 Python 对象
            del batch, codes_batch
            gc.collect()

        # 定期重建 DuckDB 连接，释放 C++ 层缓存
        if (i // BATCH_SIZE + 1) % RECONNECT_INTERVAL == 0:
            logger.info("🔄 重建 DuckDB 连接，释放内部缓存...")
            con.close()
            con = get_connection()

    logger.success(f"🎉 全部完成，共更新 {total_inserted} 行")

    total = con.execute("SELECT COUNT(*) FROM bs_indicators").fetchone()[0]
    logger.info(f"bs_indicators 总行数: {total}")
    con.close()


if __name__ == "__main__":
    main()
