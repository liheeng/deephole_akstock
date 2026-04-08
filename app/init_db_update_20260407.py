import duckdb
import os
from db.db_common import DB
from utils.common import is_running_in_docker
DB_PATH = DB


def update_db():

    data_dir = "/data" if is_running_in_docker() else "./data"
    os.makedirs(data_dir, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    print("Update database...")

    con.execute("""
    ALTER TABLE stock_indicators ADD COLUMN ema12 DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN ema26 DOUBLE;

    ALTER TABLE stock_indicators ADD COLUMN macd DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN macd_signal DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN macd_hist DOUBLE;

    ALTER TABLE stock_indicators ADD COLUMN k DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN d DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN j DOUBLE;

    ALTER TABLE stock_indicators ADD COLUMN vol_ma5 DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN vol_ma10 DOUBLE;

    ALTER TABLE stock_indicators ADD COLUMN obv DOUBLE;

    ALTER TABLE stock_indicators ADD COLUMN ret_1d DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN ret_5d DOUBLE;
    ALTER TABLE stock_indicators ADD COLUMN ret_20d DOUBLE;

    ALTER TABLE stock_indicators ADD COLUMN pct_from_ma20 DOUBLE;
    """)

    con.execute("""
    CREATE INDEX idx_stock_indicators_symbol_date
    ON stock_indicators(symbol, date);
    """)

    con.execute("""
    CREATE TABLE stock_factor_scores (
        symbol VARCHAR,
        date DATE,

        -- ==============================
        -- 趋势因子（Trend）
        -- ==============================
        trend_ma5 DOUBLE,           -- (close - ma5) / ma5（超短趋势）
        trend_ma10 DOUBLE,          -- (close - ma10) / ma10（短期趋势）
        trend_ma20 DOUBLE,          -- 中短期趋势
        trend_ma60 DOUBLE,          -- 中期趋势
        trend_macd DOUBLE,          -- MACD动量
                
        -- ==============================
        -- 动量因子（Momentum）
        -- ==============================
        mom_5d DOUBLE,
        mom_20d DOUBLE,
        mom_60d DOUBLE,
        rsi_factor DOUBLE,

        -- ==============================
        -- 波动率（Volatility）
        -- ==============================
        vol_atr DOUBLE,
        vol_boll_width DOUBLE,

        -- ==============================
        -- 成交量（Volume）
        -- ==============================
        vol_ratio DOUBLE,
        obv_slope DOUBLE,

        -- ==============================
        -- 价格位置
        -- ==============================
        price_position DOUBLE,

        -- ==============================
        -- 聚合评分
        -- ==============================
        trend_score DOUBLE,
        momentum_score DOUBLE,
        volatility_score DOUBLE,
        volume_score DOUBLE,
        composite_score DOUBLE,

        -- ===== 趋势增强 =====
        trend_acceleration DOUBLE,        -- ma5 - ma10（趋势加速）

        -- ===== 信号（Signal Layer）=====
        acc_signal BOOLEAN,               -- 趋势加速 > 0
        trend_strong BOOLEAN,             -- 多头排列
        trend_weak BOOLEAN,               -- 空头排列

        -- ===== 动量增强 =====
        momentum_acceleration DOUBLE,     -- mom_5d - mom_20d
        momentum_strong BOOLEAN,

        -- ===== 波动过滤 =====
        low_volatility BOOLEAN,           -- 低波动（适合突破）
        high_volatility BOOLEAN,          -- 高波动（风险）

        -- ===== 成交量增强 =====
        volume_spike BOOLEAN,             -- 放量确认
        volume_trend BOOLEAN,             -- 持续放量

        -- ===== 综合信号 =====
        breakout_confirm BOOLEAN,         -- 突破 + 放量 + 趋势
        reversal_signal BOOLEAN,           -- 超跌反弹
                        
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (symbol, date)
    );
    """)

    con.execute("""
    CREATE INDEX idx_stock_factor_scores_symbol_date
    ON stock_factor_scores(symbol, date);
    """)

    con.execute("""
    ALTER TABLE stock_factors RENAME TO stock_signals;
    """)
    con.close()

    print("Database updated!")


if __name__ == "__main__":
    update_db()
