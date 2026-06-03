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
    """获取需要更新的股票代码及其最后信号日期。

    拆成两次独立 GROUP BY + pandas merge，避免大表 LEFT JOIN。
    """
    # 1) kline_day 每只股票的最大日期
    daily_df = con.execute("""
        SELECT code, MAX(date::DATE) AS daily_max
        FROM kline_day
        GROUP BY code
        ORDER BY code
    """).df()

    # 2) bs_signals 每只股票的最大日期
    sig_df = con.execute("""
        SELECT symbol, MAX(date) AS signal_max
        FROM bs_signals
        GROUP BY symbol
    """).df()

    # 3) pandas merge（内存操作，极快）
    merged = daily_df.merge(
        sig_df, left_on="code", right_on="symbol", how="left"
    )
    stale = merged[
        merged["signal_max"].isna() | (merged["daily_max"] > merged["signal_max"])
    ]
    return [
        (r.code, r.daily_max, r.signal_max if pd.notna(r.signal_max) else None)
        for r in stale.itertuples()
    ]


def build_stale_map(stale_codes: list) -> dict:
    """构建 {symbol: last_signal_date_or_None} 的快速查找表"""
    return {r[0]: r[2] for r in stale_codes}


BATCH_SIZE = 15  # 每批 15 只
LOOKBACK = 200   # 增量更新时读取最近 N 根 K 线


def process_batch(con, codes_batch: list[str], stale_map: dict):
    """批量处理信号计算（自适应：新股全量，老股只读最近K线）

    批量读取 kline_day + bs_indicators，per-symbol 计算信号，
    增量过滤后批量写入。
    """
    new_codes = [c for c in codes_batch if stale_map.get(c) is None]
    update_codes = [c for c in codes_batch if stale_map.get(c) is not None]

    frames = []

    # ---- 新股：读取全量 K 线 + 指标 ----
    if new_codes:
        in_new = ",".join(f"'{c}'" for c in new_codes)
        df_new = con.execute(f"""
            SELECT
                d.code AS symbol, d.date::DATE AS date,
                d.open, d.high, d.low, d.close, d.volume,
                i.ma5, i.ma10, i.ma20, i.ma60,
                i.rsi14, i.atr14,
                i.boll_up, i.boll_down,
                i.vol_ma5, i.vol_ma10, i.vol_ma20
            FROM kline_day d
            JOIN bs_indicators i ON d.code = i.symbol AND d.date::DATE = i.date
            WHERE d.code IN ({in_new})
            ORDER BY d.code, d.date
        """).df()
        frames.append(df_new)

    # ---- 老股：只读最近 LOOKBACK 根 K 线 + 指标 ----
    if update_codes:
        in_update = ",".join(f"'{c}'" for c in update_codes)
        df_update = con.execute(f"""
            SELECT symbol, date, open, high, low, close, volume,
                   ma5, ma10, ma20, ma60,
                   rsi14, atr14,
                   boll_up, boll_down,
                   vol_ma5, vol_ma10, vol_ma20
            FROM (
                SELECT
                    d.code AS symbol, d.date::DATE AS date,
                    d.open, d.high, d.low, d.close, d.volume,
                    i.ma5, i.ma10, i.ma20, i.ma60,
                    i.rsi14, i.atr14,
                    i.boll_up, i.boll_down,
                    i.vol_ma5, i.vol_ma10, i.vol_ma20,
                    ROW_NUMBER() OVER (PARTITION BY d.code ORDER BY d.date DESC) AS rn
                FROM kline_day d
                JOIN bs_indicators i ON d.code = i.symbol AND d.date::DATE = i.date
                WHERE d.code IN ({in_update})
            )
            WHERE rn <= {LOOKBACK}
            ORDER BY symbol, date
        """).df()
        frames.append(df_update)

    if not frames:
        return 0

    df_all = pd.concat(frames, ignore_index=True)
    del frames

    if df_all.empty:
        return 0

    # ---- per-symbol 计算信号 + 增量过滤 ----
    calc = SignalsCalculator()
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

    results = []
    for sym in codes_batch:
        sym_df = df_all[df_all["symbol"] == sym].copy()
        if sym_df.empty:
            continue

        sym_df = calc.calculate(sym_df)

        signal_max = stale_map.get(sym)
        if signal_max is not None:
            sym_df = sym_df[pd.to_datetime(sym_df["date"]) > pd.Timestamp(signal_max)]

        if not sym_df.empty:
            results.append(sym_df[signal_cols])

    if not results:
        return 0

    df_out = pd.concat(results, ignore_index=True)
    n = len(df_out)

    # ---- 批量写入 ----
    con.register("tmp_signals", df_out)
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
    del df_all, df_out, results
    return n


RECONNECT_INTERVAL = 30  # 每 30 批重建连接，防内存累积


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
    stale_map = build_stale_map(stale)
    logger.info(f"需要更新的股票数: {len(stale)}")

    total_inserted = 0
    for i in range(0, len(stale), BATCH_SIZE):
        batch = stale[i:i + BATCH_SIZE]
        codes_batch = [r[0] for r in batch]
        try:
            inserted = process_batch(con, codes_batch, stale_map)
            total_inserted += inserted
            logger.info(f"✅ 批次 {i//BATCH_SIZE + 1}/{(len(stale)-1)//BATCH_SIZE + 1}: {len(codes_batch)} 只, 更新 {inserted} 行")
        except Exception:
            logger.exception(f"❌ 批次 {i//BATCH_SIZE + 1} 失败，跳过该批")
            continue
        finally:
            del batch, codes_batch
            gc.collect()

        if (i // BATCH_SIZE + 1) % RECONNECT_INTERVAL == 0:
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
