"""
根据 kline_day + bs_indicators 计算 bs_signals 信号表。

信号定义与 stock_signals 完全一致：
  趋势类, RSI超买超卖, 突破/跌破, ATR波动, 布林带,
  成交量异动, 连续涨跌, 加速/多头/空头排列, 动量, 
  波动率, 放量趋势, 突破确认, 反转信号

用法:
  python update_baostock_signals.py
"""

import duckdb
import gc
import os
import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm
from utils.common import is_running_in_docker

BAOSTOCK_HIS_DB_PATH = os.environ.get(
    "BAOSTOCK_HIS_DB_PATH", "/data" if is_running_in_docker() else "./data"
)
DB_PATH = BAOSTOCK_HIS_DB_PATH + "/baostock_data.duckdb"

logger.add("./logs/baostock_signals.log", rotation="100 MB", encoding="utf-8", enqueue=True)


# ======================== 表结构 ========================
BS_SIGNALS_DDL = """
    CREATE TABLE IF NOT EXISTS bs_signals (
        symbol VARCHAR,
        date DATE,

        -- 趋势类
        ma5_above_ma20 BOOLEAN,
        ma20_above_ma60 BOOLEAN,
        close_above_ma20 BOOLEAN,

        -- 动量类
        rsi_overbought BOOLEAN,
        rsi_oversold BOOLEAN,

        -- 突破类
        breakout_20d BOOLEAN,
        breakdown_20d BOOLEAN,

        -- 波动类
        atr_high_vol BOOLEAN,
        boll_upper_break BOOLEAN,
        boll_lower_break BOOLEAN,

        -- 成交量
        vol_spike BOOLEAN,
        vol_ma5_above_ma20 BOOLEAN,

        -- 连续性
        up_3days BOOLEAN,
        down_3days BOOLEAN,

        acc_signal BOOLEAN,
        trend_strong BOOLEAN,
        trend_weak BOOLEAN,
        momentum_strong BOOLEAN,
        low_volatility BOOLEAN,
        high_volatility BOOLEAN,
        volume_spike BOOLEAN,
        volume_trend BOOLEAN,
        breakout_confirm BOOLEAN,
        reversal_signal BOOLEAN,

        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_bs_signals_symbol_date
    ON bs_signals(symbol, date);
"""


# ======================== 信号计算（同 update_signals.py） ========================
class SignalsCalculator:

    @staticmethod
    def safe_gt(a: pd.Series, b: pd.Series) -> pd.Series:
        result = pd.Series(pd.NA, index=a.index, dtype="boolean")
        mask = a.notna() & b.notna()
        result.loc[mask] = a.loc[mask] > b.loc[mask]
        return result

    @staticmethod
    def safe_lt(a: pd.Series, b: pd.Series) -> pd.Series:
        result = pd.Series(pd.NA, index=a.index, dtype="boolean")
        mask = a.notna() & b.notna()
        result.loc[mask] = a.loc[mask] < b.loc[mask]
        return result

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date").copy()

        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # ---- 趋势 ----
        df["ma5_above_ma20"] = self.safe_gt(df["ma5"], df["ma20"])
        df["ma20_above_ma60"] = self.safe_gt(df["ma20"], df["ma60"])
        df["close_above_ma20"] = self.safe_gt(df["close"], df["ma20"])

        # ---- RSI ----
        df["rsi_overbought"] = self.safe_gt(df["rsi14"], pd.Series(70, index=df.index))
        df["rsi_oversold"] = self.safe_lt(df["rsi14"], pd.Series(30, index=df.index))

        # ---- Breakout / Breakdown ----
        df["hhv20"] = df["high"].rolling(20, min_periods=20).max().shift(1)
        df["llv20"] = df["low"].rolling(20, min_periods=20).min().shift(1)

        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = df["close"].notna() & df["hhv20"].notna()
        result.loc[mask] = df.loc[mask, "close"] >= df.loc[mask, "hhv20"]
        df["breakout_20d"] = result

        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = df["close"].notna() & df["llv20"].notna()
        result.loc[mask] = df.loc[mask, "close"] <= df.loc[mask, "llv20"]
        df["breakdown_20d"] = result

        # ---- ATR Volatility ----
        df["atr_mean"] = df["atr14"].rolling(20, min_periods=20).mean()
        df["atr_high_vol"] = self.safe_gt(df["atr14"], df["atr_mean"])

        # ---- Bollinger ----
        df["boll_upper_break"] = self.safe_gt(df["close"], df["boll_up"])
        df["boll_lower_break"] = self.safe_lt(df["close"], df["boll_down"])

        # ---- Volume ----
        df["vol_ma5_calc"] = df["volume"].rolling(5, min_periods=5).mean()
        df["vol_ma20_calc"] = df["volume"].rolling(20, min_periods=20).mean()

        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        vol_base = df["vol_ma5_calc"].replace(0, np.nan)
        mask = df["volume"].notna() & vol_base.notna()
        result.loc[mask] = df.loc[mask, "volume"] > vol_base.loc[mask] * 1.5
        df["vol_spike"] = result

        df["vol_ma5_above_ma20"] = self.safe_gt(df["vol_ma5_calc"], df["vol_ma20_calc"])

        # ---- Factor-style Signals ----
        # Trend Acceleration
        trend_acc = df["ma5"] - df["ma10"]
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = df["ma5"].notna() & df["ma10"].notna()
        result.loc[mask] = trend_acc.loc[mask] > 0
        df["acc_signal"] = result

        # Trend Strong（多头排列）
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = df["ma5"].notna() & df["ma10"].notna() & df["ma20"].notna() & df["ma60"].notna()
        result.loc[mask] = (
            (df.loc[mask, "ma5"] > df.loc[mask, "ma10"])
            & (df.loc[mask, "ma10"] > df.loc[mask, "ma20"])
            & (df.loc[mask, "ma20"] > df.loc[mask, "ma60"])
        )
        df["trend_strong"] = result

        # Trend Weak（空头排列）
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = df["ma5"].notna() & df["ma10"].notna() & df["ma20"].notna()
        result.loc[mask] = (
            (df.loc[mask, "ma5"] < df.loc[mask, "ma10"])
            & (df.loc[mask, "ma10"] < df.loc[mask, "ma20"])
        )
        df["trend_weak"] = result

        # Momentum Strong
        ret5 = df["close"].ffill().pct_change(5)
        ret20 = df["close"].ffill().pct_change(20)
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = ret5.notna() & ret20.notna()
        result.loc[mask] = (ret5.loc[mask] > 0) & (ret20.loc[mask] > 0)
        df["momentum_strong"] = result

        # Volatility Regime
        vol_atr = df["atr14"] / df["close"].replace(0, np.nan)
        atr_mean = vol_atr.rolling(20, min_periods=20).mean()
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = vol_atr.notna() & atr_mean.notna()
        result.loc[mask] = vol_atr.loc[mask] < atr_mean.loc[mask]
        df["low_volatility"] = result
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        result.loc[mask] = vol_atr.loc[mask] > atr_mean.loc[mask] * 1.5
        df["high_volatility"] = result

        # Volume Spike
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        vol_base = df["vol_ma5_calc"].replace(0, np.nan)
        mask = df["volume"].notna() & vol_base.notna()
        result.loc[mask] = df.loc[mask, "volume"] > vol_base.loc[mask] * 1.5
        df["volume_spike"] = result

        # Volume Trend
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = df["vol_ma5_calc"].notna() & df["vol_ma10"].notna() & df["vol_ma20"].notna()
        result.loc[mask] = (
            (df.loc[mask, "vol_ma5_calc"] > df.loc[mask, "vol_ma10"])
            & (df.loc[mask, "vol_ma10"] > df.loc[mask, "vol_ma20"])
        )
        df["volume_trend"] = result

        # Breakout Confirm
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        boll_up_prev = df["boll_up"].shift(1)
        mask = df["close"].notna() & boll_up_prev.notna() & df["volume_spike"].notna() & df["acc_signal"].notna()
        result.loc[mask] = (
            (df.loc[mask, "close"] > boll_up_prev.loc[mask])
            & (df.loc[mask, "volume_spike"])
            & (df.loc[mask, "acc_signal"])
        )
        df["breakout_confirm"] = result

        # Reversal Signal
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        boll_down_prev = df["boll_down"].shift(1)
        mask = df["rsi14"].notna() & df["close"].notna() & boll_down_prev.notna() & df["acc_signal"].notna()
        result.loc[mask] = (
            (df.loc[mask, "rsi14"] < 30)
            & (df.loc[mask, "close"] < boll_down_prev.loc[mask])
            & (df.loc[mask, "acc_signal"])
        )
        df["reversal_signal"] = result

        # 连续涨跌
        _close = df["close"].ffill()
        ret = _close.pct_change()
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        mask = ret.notna() & ret.shift(1).notna() & ret.shift(2).notna()
        result.loc[mask] = (ret.loc[mask] > 0) & (ret.shift(1).loc[mask] > 0) & (ret.shift(2).loc[mask] > 0)
        df["up_3days"] = result
        result = pd.Series(pd.NA, index=df.index, dtype="boolean")
        result.loc[mask] = (ret.loc[mask] < 0) & (ret.shift(1).loc[mask] < 0) & (ret.shift(2).loc[mask] < 0)
        df["down_3days"] = result

        # 清理临时列
        df = df.drop(columns=["hhv20", "llv20", "atr_mean", "vol_ma5_calc", "vol_ma20_calc", "ret"], errors="ignore")
        return df


# ======================== 核心逻辑 ========================
def get_stale_codes(con):
    """获取需要更新的股票代码列表"""
    rows = con.execute("""
        SELECT
            d.code,
            MAX(d.date::DATE) AS daily_max,
            MAX(s.date) AS signal_max
        FROM kline_day d
        LEFT JOIN bs_signals s ON d.code = s.symbol
        GROUP BY d.code
        HAVING signal_max IS NULL OR MAX(d.date::DATE) > MAX(s.date)
        ORDER BY d.code
    """).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def update_code(con, code: str, daily_max, signal_max, lookback: int = 200):
    """更新单只股票的信号"""
    if signal_max is None:
        df = con.execute("""
            SELECT
                d.code AS symbol, d.date, d.open, d.high, d.low, d.close, d.volume,
                i.ma5, i.ma10, i.ma20, i.ma60,
                i.rsi14, i.atr14,
                i.boll_up, i.boll_down,
                i.vol_ma5, i.vol_ma10, i.vol_ma20
            FROM kline_day d
            JOIN bs_indicators i ON d.code = i.symbol AND d.date = i.date
            WHERE d.code = ?
            ORDER BY d.date
        """, [code]).df()
    else:
        df = con.execute("""
            SELECT * FROM (
                SELECT
                    d.code AS symbol, d.date, d.open, d.high, d.low, d.close, d.volume,
                    i.ma5, i.ma10, i.ma20, i.ma60,
                    i.rsi14, i.atr14,
                    i.boll_up, i.boll_down,
                    i.vol_ma5, i.vol_ma10, i.vol_ma20
                FROM kline_day d
                JOIN bs_indicators i ON d.code = i.symbol AND d.date = i.date
                WHERE d.code = ?
                ORDER BY d.date DESC
                LIMIT ?
            )
            ORDER BY date
        """, [code, lookback]).df()

    if df.empty:
        logger.warning(f"{code}: 无数据")
        return 0

    calc = SignalsCalculator()
    df = calc.calculate(df)

    if signal_max is not None:
        df["date_parsed"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date_parsed"] > signal_max]
        df = df.drop(columns=["date_parsed"])

    if df.empty:
        return 0

    signal_cols = [
        "symbol", "date",
        "ma5_above_ma20", "ma20_above_ma60", "close_above_ma20",
        "rsi_overbought", "rsi_oversold",
        "breakout_20d", "breakdown_20d",
        "atr_high_vol",
        "boll_upper_break", "boll_lower_break",
        "vol_spike", "vol_ma5_above_ma20",
        "up_3days", "down_3days",
        "acc_signal", "trend_strong", "trend_weak",
        "momentum_strong", "low_volatility", "high_volatility",
        "volume_spike", "volume_trend",
        "breakout_confirm", "reversal_signal",
    ]
    df = df[signal_cols]

    con.register("tmp_signals", df)
    con.execute("""
        INSERT OR REPLACE INTO bs_signals (
            symbol, date,
            ma5_above_ma20, ma20_above_ma60, close_above_ma20,
            rsi_overbought, rsi_oversold,
            breakout_20d, breakdown_20d,
            atr_high_vol,
            boll_upper_break, boll_lower_break,
            vol_spike, vol_ma5_above_ma20,
            up_3days, down_3days,
            acc_signal, trend_strong, trend_weak,
            momentum_strong, low_volatility, high_volatility,
            volume_spike, volume_trend,
            breakout_confirm, reversal_signal,
            create_time, update_time
        )
        SELECT
            symbol, date,
            ma5_above_ma20, ma20_above_ma60, close_above_ma20,
            rsi_overbought, rsi_oversold,
            breakout_20d, breakdown_20d,
            atr_high_vol,
            boll_upper_break, boll_lower_break,
            vol_spike, vol_ma5_above_ma20,
            up_3days, down_3days,
            acc_signal, trend_strong, trend_weak,
            momentum_strong, low_volatility, high_volatility,
            volume_spike, volume_trend,
            breakout_confirm, reversal_signal,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM tmp_signals
    """)
    con.unregister("tmp_signals")
    n = len(df)
    del df
    return n


RECONNECT_INTERVAL = 100  # 每 100 只重建连接，防内存累积


# ======================== 主函数 ========================
def main():
    con = duckdb.connect(DB_PATH)
    con.execute("SET memory_limit = '2GB'")

    # 创建 bs_signals 表
    for stmt in BS_SIGNALS_DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt + ";")

    # 获取需要更新的股票
    stale = get_stale_codes(con)
    logger.info(f"需要更新的股票数: {len(stale)}")

    total_inserted = 0
    for idx, (code, daily_max, signal_max) in enumerate(tqdm(stale, desc="计算信号进度")):
        try:
            inserted = update_code(con, code, daily_max, signal_max)
            total_inserted += inserted
            if inserted > 0:
                logger.info(f"✅ {code}: 更新 {inserted} 行")
        except Exception:
            logger.exception(f"❌ {code}: 更新失败，跳过")
            continue
        finally:
            gc.collect()

        if (idx + 1) % RECONNECT_INTERVAL == 0:
            logger.info("🔄 重建 DuckDB 连接，释放内部缓存...")
            con.close()
            con = duckdb.connect(DB_PATH)
            con.execute("SET memory_limit = '2GB'")

    logger.success(f"🎉 全部完成，共更新 {total_inserted} 行")
    total = con.execute("SELECT COUNT(*) FROM bs_signals").fetchone()[0]
    logger.info(f"bs_signals 总行数: {total}")
    con.close()


if __name__ == "__main__":
    main()
