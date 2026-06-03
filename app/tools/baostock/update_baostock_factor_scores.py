"""
根据 kline_day + bs_indicators 计算 bs_factor_scores 因子分数表。

因子定义与 stock_factor_scores 完全一致：
  趋势因子, 动量因子, 波动率因子, 成交量因子, 价格位置,
  各维度评分, 综合评分, 信号层

依赖: 先运行 update_baostock_indicators.py 确保 bs_indicators 已更新

用法:
  python update_baostock_factor_scores.py
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

logger.add("./logs/baostock_factor_scores.log", rotation="100 MB", encoding="utf-8", enqueue=True)


# ======================== 表结构 ========================
BS_FACTOR_SCORES_DDL = """
    CREATE TABLE IF NOT EXISTS bs_factor_scores (
        symbol VARCHAR,
        date DATE,

        -- 趋势因子
        trend_ma5 DOUBLE, trend_ma10 DOUBLE, trend_ma20 DOUBLE, trend_ma60 DOUBLE,
        trend_macd DOUBLE,

        -- 动量因子
        mom_5d DOUBLE, mom_20d DOUBLE, mom_60d DOUBLE, rsi_factor DOUBLE,

        -- 波动率
        vol_atr DOUBLE, vol_boll_width DOUBLE,

        -- 成交量
        vol_ratio DOUBLE, obv_slope DOUBLE,

        -- 价格位置
        price_position DOUBLE,

        -- 聚合评分
        trend_score DOUBLE, momentum_score DOUBLE,
        volatility_score DOUBLE, volume_score DOUBLE,
        composite_score DOUBLE,

        -- 趋势增强
        trend_acceleration DOUBLE,

        -- 信号
        acc_signal BOOLEAN, trend_strong BOOLEAN, trend_weak BOOLEAN,

        -- 动量增强
        momentum_acceleration DOUBLE, momentum_strong BOOLEAN,

        -- 波动过滤
        low_volatility BOOLEAN, high_volatility BOOLEAN,

        -- 成交量增强
        volume_spike BOOLEAN, volume_trend BOOLEAN,

        -- 综合信号
        breakout_confirm BOOLEAN, reversal_signal BOOLEAN,

        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_bs_factor_scores_symbol_date
    ON bs_factor_scores(symbol, date);
"""


# ======================== 因子计算（同 update_factor_scores.py） ========================
class FactorScoresCalculator:
    def __init__(self):
        self.factor_cols = [
            "trend_ma5", "trend_ma10", "trend_ma20", "trend_ma60",
            "trend_macd",
            "mom_5d", "mom_20d", "mom_60d",
            "rsi_factor",
            "vol_atr", "vol_boll_width",
            "vol_ratio", "obv_slope",
            "price_position"
        ]

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date").copy()
        close = df["close"]

        # ---- 原始因子 ----
        df["trend_ma5"] = (close - df["ma5"]) / df["ma5"]
        df["trend_ma10"] = (close - df["ma10"]) / df["ma10"]
        df["trend_ma20"] = (close - df["ma20"]) / df["ma20"]
        df["trend_ma60"] = (close - df["ma60"]) / df["ma60"]
        df["trend_macd"] = df["macd_hist"]

        df["mom_5d"] = df["ret_5d"]
        df["mom_20d"] = df["ret_20d"]
        df["mom_60d"] = close.pct_change(60)
        df["rsi_factor"] = (df["rsi14"] - 50) / 50

        df["vol_atr"] = df["atr14"] / close
        df["vol_boll_width"] = (df["boll_up"] - df["boll_down"]) / close

        df["vol_ratio"] = df["volume"] / df["vol_ma20"]
        df["obv_slope"] = df["obv"].diff(5) / 5

        df["price_position"] = df["pct_from_ma20"]

        # ---- 标准化（按日期分组 z-score，用内置函数替代 lambda，快 ~50x） ----
        for col in self.factor_cols:
            grp = df.groupby("date")[col]
            mean = grp.transform("mean")
            std = grp.transform("std")
            df[col] = (df[col] - mean) / (std + 1e-9)

        # ---- 二次因子 ----
        df["trend_acceleration"] = df["trend_ma5"] - df["trend_ma10"]
        df["momentum_acceleration"] = df["mom_5d"] - df["mom_20d"]

        # ---- 各维度评分 ----
        df["trend_score"] = (
            df["trend_ma5"] * 0.20 + df["trend_ma10"] * 0.20 +
            df["trend_ma20"] * 0.15 + df["trend_ma60"] * 0.10 +
            df["trend_macd"] * 0.15 + df["trend_acceleration"] * 0.20
        )
        df["momentum_score"] = (
            df["mom_5d"] * 0.25 + df["mom_20d"] * 0.45 +
            df["mom_60d"] * 0.20 + df["rsi_factor"] * 0.10
        )
        df["volatility_score"] = df["vol_atr"] * 0.50 + df["vol_boll_width"] * 0.50
        df["volume_score"] = df["vol_ratio"] * 0.70 + df["obv_slope"] * 0.30
        df["composite_score"] = (
            df["trend_score"] * 0.35 + df["momentum_score"] * 0.40 +
            df["volume_score"] * 0.20 - df["volatility_score"] * 0.05
        )

        # ---- 信号层 ----
        df["acc_signal"] = df["trend_acceleration"] > 0
        df["trend_strong"] = (
            (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"]) & (df["ma20"] > df["ma60"])
        )
        df["trend_weak"] = (df["ma5"] < df["ma10"]) & (df["ma10"] < df["ma20"])
        df["momentum_strong"] = (df["mom_5d"] > 0) & (df["mom_20d"] > 0)

        atr_mean = df["vol_atr"].rolling(20, min_periods=1).mean()
        df["low_volatility"] = df["vol_atr"] < atr_mean
        df["high_volatility"] = df["vol_atr"] > atr_mean * 1.5

        df["volume_spike"] = df["volume"] > df["vol_ma5"] * 1.5
        df["volume_trend"] = (
            (df["vol_ma5"] > df["vol_ma10"]) & (df["vol_ma10"] > df["vol_ma20"])
        )
        df["breakout_confirm"] = (
            (df["close"] > df["boll_up"]) & df["volume_spike"] & df["acc_signal"]
        )
        df["reversal_signal"] = (
            (df["rsi14"] < 30) & (df["close"] < df["boll_down"]) & df["acc_signal"]
        )

        return df


# ======================== 核心逻辑 ========================
def get_stale_codes(con):
    """获取需要更新的股票代码列表"""
    rows = con.execute("""
        SELECT
            d.code,
            MAX(d.date::DATE) AS daily_max,
            MAX(f.date) AS factor_max
        FROM kline_day d
        LEFT JOIN bs_factor_scores f ON d.code = f.symbol
        GROUP BY d.code
        HAVING factor_max IS NULL OR MAX(d.date::DATE) > MAX(f.date)
        ORDER BY d.code
    """).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


BATCH_SIZE = 100  # 每批 100 只，保证 groupby(date) 有足够样本算 z-score


def process_batch(con, codes_batch: list[str]):
    """批量计算因子分数（多只股票一起算，groupby(date) 有意义）"""
    quoted = [f"'{c}'" for c in codes_batch]
    in_clause = ",".join(quoted)

    df = con.execute(f"""
        SELECT
            d.code AS symbol, d.date::DATE AS date,
            d.close, d.volume,
            i.ma5, i.ma10, i.ma20, i.ma60,
            i.macd_hist,
            i.rsi14, i.atr14,
            i.boll_up, i.boll_down,
            i.vol_ma5, i.vol_ma10, i.vol_ma20,
            i.obv,
            i.ret_5d, i.ret_20d, i.pct_from_ma20
        FROM kline_day d
        JOIN bs_indicators i ON d.code = i.symbol AND d.date::DATE = i.date
        WHERE d.code IN ({in_clause})
        ORDER BY d.code, d.date
    """).df()

    if df.empty:
        return 0

    # 批量计算因子（groupby(date) 有多个股票，z-score 有意义）
    calc = FactorScoresCalculator()
    df = calc.calculate(df)

    # 写入
    out_cols = [
        "symbol", "date",
        "trend_ma5", "trend_ma10", "trend_ma20", "trend_ma60", "trend_macd",
        "trend_acceleration",
        "mom_5d", "mom_20d", "mom_60d", "momentum_acceleration", "rsi_factor",
        "vol_atr", "vol_boll_width",
        "vol_ratio", "obv_slope",
        "price_position",
        "trend_score", "momentum_score", "volatility_score", "volume_score",
        "composite_score",
        "acc_signal", "trend_strong", "trend_weak",
        "momentum_strong",
        "low_volatility", "high_volatility",
        "volume_spike", "volume_trend",
        "breakout_confirm", "reversal_signal",
    ]
    df = df[out_cols]

    con.register("tmp_factors", df)
    con.execute("""
        INSERT OR IGNORE INTO bs_factor_scores (
            symbol, date,
            trend_ma5, trend_ma10, trend_ma20, trend_ma60, trend_macd,
            trend_acceleration,
            mom_5d, mom_20d, mom_60d, momentum_acceleration, rsi_factor,
            vol_atr, vol_boll_width,
            vol_ratio, obv_slope,
            price_position,
            trend_score, momentum_score, volatility_score, volume_score,
            composite_score,
            acc_signal, trend_strong, trend_weak,
            momentum_strong,
            low_volatility, high_volatility,
            volume_spike, volume_trend,
            breakout_confirm, reversal_signal
        )
        SELECT
            symbol, date,
            trend_ma5, trend_ma10, trend_ma20, trend_ma60, trend_macd,
            trend_acceleration,
            mom_5d, mom_20d, mom_60d, momentum_acceleration, rsi_factor,
            vol_atr, vol_boll_width,
            vol_ratio, obv_slope,
            price_position,
            trend_score, momentum_score, volatility_score, volume_score,
            composite_score,
            acc_signal, trend_strong, trend_weak,
            momentum_strong,
            low_volatility, high_volatility,
            volume_spike, volume_trend,
            breakout_confirm, reversal_signal
        FROM tmp_factors
    """)
    con.unregister("tmp_factors")
    n = len(df)
    del df
    return n


RECONNECT_INTERVAL = 5  # 每 5 批（~50秒）重建连接，防内存累积


# ======================== 主函数 ========================
def main():
    con = duckdb.connect(DB_PATH)
    con.execute("SET memory_limit = '2GB'")

    # 创建 bs_factor_scores 表
    for stmt in BS_FACTOR_SCORES_DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt + ";")

    # 获取需要更新的股票
    stale = [r[0] for r in get_stale_codes(con)]
    logger.info(f"需要更新的股票数: {len(stale)}")

    total_inserted = 0
    for i in range(0, len(stale), BATCH_SIZE):
        codes_batch = stale[i:i + BATCH_SIZE]
        try:
            inserted = process_batch(con, codes_batch)
            total_inserted += inserted
            logger.info(f"✅ 批次 {i//BATCH_SIZE + 1}/{(len(stale)-1)//BATCH_SIZE + 1}: {len(codes_batch)} 只, 更新 {inserted} 行")
        except Exception:
            logger.exception(f"❌ 批次 {i//BATCH_SIZE + 1} 失败，跳过该批")
            continue
        finally:
            gc.collect()

        if (i // BATCH_SIZE + 1) % RECONNECT_INTERVAL == 0:
            logger.info("🔄 重建 DuckDB 连接，释放内部缓存...")
            con.close()
            con = duckdb.connect(DB_PATH)
            con.execute("SET memory_limit = '2GB'")

    logger.success(f"🎉 全部完成，共更新 {total_inserted} 行")
    total = con.execute("SELECT COUNT(*) FROM bs_factor_scores").fetchone()[0]
    logger.info(f"bs_factor_scores 总行数: {total}")
    con.close()


if __name__ == "__main__":
    main()
