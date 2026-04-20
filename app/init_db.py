import duckdb
import os
from db.db_common import DB
from utils.common import is_running_in_docker
DB_PATH = DB


def init_db():

    data_dir = "/data" if is_running_in_docker() else "./data"
    os.makedirs(data_dir, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    # 检查是否已经初始化
    tables = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'stock_daily'
    """).fetchall()

    if tables:
        print("DB already initialized, skip.")
        con.close()
        return

    print("Initializing database...")

    con.execute("""
    CREATE TABLE stock_daily (
        symbol VARCHAR,        -- 股票代码
        symbol_name VARCHAR,          -- 股票名称
        market VARCHAR,        -- 市场
        date DATE,             -- 日期
        open DOUBLE,           -- 开盘价
        high DOUBLE,           -- 最高价
        low DOUBLE,            -- 最低价
        close DOUBLE,          -- 收盘价
        volume DOUBLE,         -- 成交量
        amount DOUBLE,         -- 成交额
        pct DOUBLE,            -- 涨跌幅
        turnover DOUBLE,       -- 换手率
        adjust_mode VARCHAR DEFAULT 'none',   -- 复权模式
        adjust_factor DOUBLE,    -- 复权因子
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 创建时间
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- 更新时间
    )
    """)

    con.execute("""
    -- 表注释
    COMMENT ON TABLE stock_daily IS '股票日线行情数据表';

    -- 列注释
    COMMENT ON COLUMN stock_daily.symbol IS '股票代码';
    COMMENT ON COLUMN stock_daily.symbol_name IS '股票名称';
    COMMENT ON COLUMN stock_daily.market IS '市场';
    COMMENT ON COLUMN stock_daily.date IS '日期';
    COMMENT ON COLUMN stock_daily.open IS '开盘价';
    COMMENT ON COLUMN stock_daily.high IS '最高价';
    COMMENT ON COLUMN stock_daily.low IS '最低价';
    COMMENT ON COLUMN stock_daily.close IS '收盘价';
    COMMENT ON COLUMN stock_daily.volume IS '成交量';
    COMMENT ON COLUMN stock_daily.amount IS '成交额';
    COMMENT ON COLUMN stock_daily.pct IS '涨跌幅';
    COMMENT ON COLUMN stock_daily.turnover IS '换手率';
    COMMENT ON COLUMN stock_daily.adjust_mode IS '复权模式';
    COMMENT ON COLUMN stock_daily.adjust_factor IS '复权因子';
    COMMENT ON COLUMN stock_daily.create_time IS '创建时间';
    COMMENT ON COLUMN stock_daily.update_time IS '更新时间';
    """)           

    con.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uniq_sd_symbol_date
    ON stock_daily(symbol, date)
    """)

    con.execute("""
    CREATE INDEX idx_sd_market_date
    ON stock_daily(market, date)
    """)

    con.execute("""
    CREATE TABLE market_info (
        market VARCHAR PRIMARY KEY,
        description VARCHAR
    )
    """)

    con.execute("""
    INSERT INTO market_info VALUES
    ('CN', 'China A Shares'),
    ('HK', 'Hong Kong Stocks'),
    ('US', 'US Stocks'),
    ('FUT', 'Futures')
    """)

    con.execute("""
    CREATE TABLE stock_indicators (
        -- ==============================
        -- 基础字段
        -- ==============================
        symbol VARCHAR,              -- 股票代码
        date DATE,                   -- 交易日期

        -- ==============================
        -- 趋势类（Trend Indicators）
        -- ==============================
        ma5 DOUBLE,                  -- 5日简单移动平均（短期趋势）
        ma10 DOUBLE,                 -- 10日均线
        ma20 DOUBLE,                 -- 20日均线（中短期趋势）
        ma60 DOUBLE,                 -- 60日均线（中期趋势）
        ma120 DOUBLE,                -- 120日均线（长期趋势）

        ema12 DOUBLE,                -- 12日指数移动平均（更敏感）
        ema26 DOUBLE,                -- 26日指数移动平均

        -- ==============================
        -- MACD（趋势 + 动量）
        -- ==============================
        macd DOUBLE,                 -- DIF = EMA12 - EMA26
        macd_signal DOUBLE,          -- DEA = MACD的9日EMA
        macd_hist DOUBLE,            -- 柱状图 = macd - macd_signal（动量强弱）

        -- ==============================
        -- 动量类（Momentum）
        -- ==============================
        rsi14 DOUBLE,                -- RSI(14)，超买超卖指标（0~100）

        k DOUBLE,                    -- KDJ中的K值（短期动量）
        d DOUBLE,                    -- KDJ中的D值（平滑K）
        j DOUBLE,                    -- J = 3K - 2D（更敏感）

        -- ==============================
        -- 波动率（Volatility）
        -- ==============================
        atr14 DOUBLE,                -- ATR(14)，真实波动幅度（无方向）

        boll_mid DOUBLE,             -- 布林带中轨（20日均线）
        boll_up DOUBLE,              -- 布林带上轨（mid + 2σ）
        boll_down DOUBLE,            -- 布林带下轨（mid - 2σ）

        -- ==============================
        -- 成交量类（Volume）
        -- ==============================
        vol_ma5 DOUBLE,              -- 5日成交量均值（短期资金活跃度）
        vol_ma10 DOUBLE,             -- 10日成交量均值

        obv DOUBLE,                  -- OBV（On Balance Volume，资金流向）

        -- ==============================
        -- 收益率（Returns）
        -- ==============================
        ret_1d DOUBLE,               -- 1日收益率
        ret_5d DOUBLE,               -- 5日收益率（短期动量）
        ret_20d DOUBLE,              -- 20日收益率（中期动量）

        -- ==============================
        -- 价格位置（Position / Mean Reversion）
        -- ==============================
        pct_from_ma20 DOUBLE,        -- 收盘价相对MA20偏离 (close - ma20) / ma20

        -- ==============================
        -- 系统字段
        -- ==============================
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 创建时间
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 更新时间

        -- ==============================
        -- 主键
        -- ==============================
        PRIMARY KEY (symbol, date)
    );
    """)

    con.execute("""
    CREATE INDEX idx_stock_indicators_symbol_date
    ON stock_indicators(symbol, date);
    """)

    con.execute("""
    CREATE TABLE stock_signals (
        symbol VARCHAR,
        date DATE,

        -- ===== 趋势类 =====
        ma5_above_ma20 BOOLEAN,
        ma20_above_ma60 BOOLEAN,
        close_above_ma20 BOOLEAN,

        -- ===== 动量类 =====
        rsi_overbought BOOLEAN,   -- RSI > 70
        rsi_oversold BOOLEAN,     -- RSI < 30

        -- ===== 突破类 =====
        breakout_20d BOOLEAN,     -- 突破20日新高
        breakdown_20d BOOLEAN,    -- 跌破20日新低

        -- ===== 波动类 =====
        atr_high_vol BOOLEAN,     -- ATR 高波动
        boll_upper_break BOOLEAN,
        boll_lower_break BOOLEAN,

        -- ===== 成交量 =====
        vol_spike BOOLEAN,        -- 放量
        vol_ma5_above_ma20 BOOLEAN,

        -- ===== 连续性 =====
        up_3days BOOLEAN,
        down_3days BOOLEAN,

        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (symbol, date)
    );
    """)

    con.execute("""
    CREATE INDEX idx_stock_signals_symbol_date
    ON stock_signals(symbol, date);
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
    CREATE TABLE stock_minute (
        symbol VARCHAR,             -- 股票代码（如 sz000001）
        symbol_name VARCHAR,        -- 股票名称
        market VARCHAR,             -- 市场 sh/sz
        period INT,                 -- 分钟周期：1/5/15/30/60
        trade_time DATETIME,        -- 分时时间（精确到分钟）
        open DOUBLE,                -- 开盘价
        high DOUBLE,                -- 最高价
        low DOUBLE,                 -- 最低价
        close DOUBLE,                -- 收盘价
        volume DOUBLE,               -- 成交量
        amount DOUBLE,               -- 成交额
        pct DOUBLE,                  -- 涨跌幅（相对于上一根K线）
        adjust_mode VARCHAR DEFAULT 'none',  -- 复权模式
        adjust_factor DOUBLE,        -- 复权因子
        
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        
         -- 分钟线唯一索引：股票 + 周期 + 时间
        UNIQUE (symbol, period, trade_time)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR PRIMARY KEY,
    description VARCHAR,
    status VARCHAR,
    mode VARCHAR,

    start_time TIMESTAMP NULL,
    execute_time TIMESTAMP NULL,
    stop_time TIMESTAMP NULL,

    message TEXT NULL,

    create_time TIMESTAMP DEFAULT now(),            
    update_time TIMESTAMP DEFAULT now())
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR PRIMARY KEY,
    type VARCHAR,
    status VARCHAR,
    task_id VARCHAR,
                
    params JSON NULL,
    depends_on VARCHAR NULL,   -- JSON string
                
    retries INTEGER,
    retry_count INTEGER,

    execute_time TIMESTAMP NULL,
    stop_time TIMESTAMP NULL,

    message TEXT NULL,
    error TEXT NULL,
                
    create_time TIMESTAMP DEFAULT now(),            
    update_time TIMESTAMP DEFAULT now())
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS running_jobs (
    job_id TEXT PRIMARY KEY,
    task_id TEXT,
    type TEXT,
    concurrency_key TEXT,
    start_time TIMESTAMP,
            
    create_time TIMESTAMP DEFAULT now(),            
    update_time TIMESTAMP DEFAULT now())
    """)

    con.execute("""
    CREATE UNIQUE INDEX idx_running_unique
    ON running_jobs(concurrency_key, job_id)
    """)

# =========================
    # 1. universe_map（指数 / 板块）
    # =========================
    con.execute("""
        CREATE TABLE IF NOT EXISTS universe_map (
            symbol        VARCHAR NOT NULL,
            universe      VARCHAR NOT NULL,
            market        VARCHAR,

            start_date    DATE NOT NULL,
            end_date      DATE,

            create_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_um_symbol
        ON universe_map(symbol);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_um_universe
        ON universe_map(universe);
    """)

    # =========================
    # 2. stock_sector（行业）
    # =========================
    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_sector (
            symbol        VARCHAR NOT NULL,
            sector        VARCHAR NOT NULL,

            start_date    DATE NOT NULL,
            end_date      DATE,

            create_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_ss_symbol
        ON stock_sector(symbol);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_ss_sector
        ON stock_sector(sector);
    """)

    # =========================
    # 3. Universe Views（全球）
    # =========================
    UNIVERSES = [
        # US
        "SP500", "NASDAQ100",
        # CN
        "HS50", "HS300", "ZZ500", "ZZ1000",
        # HK
        "HSI", "HSTECH",
        # JP
        "NIKKEI225", "TOPIX100",
        # CN Boards
        "CYB", "STAR"
    ]

    for u in UNIVERSES:
        con.execute(f"""
            CREATE OR REPLACE VIEW universe_{u.lower()} AS
            SELECT *
            FROM universe_map
            WHERE universe = '{u}';
        """)

    # =========================
    # 4. Market Views（超级重要）
    # =========================
    MARKETS = ["us", "cn", "hk", "jp"]

    for m in MARKETS:
        con.execute(f"""
            CREATE OR REPLACE VIEW universe_{m} AS
            SELECT *
            FROM universe_map
            WHERE market = '{m}';
        """)

    # =========================
    # 5. Sector Views（行业）
    # =========================
    SECTORS = {
        "SEC_FINANCE": "finance",
        "SEC_BROKER": "broker",
        "SEC_INSURANCE": "insurance",
        "SEC_AUTO": "auto",
        "SEC_MECHANICAL": "mechanical",
        "SEC_AGRI": "agri",
        "SEC_CHEMICAL": "chemical",
        "SEC_NEWENERGY": "new_energy"
    }

    for key, val in SECTORS.items():
        con.execute(f"""
            CREATE OR REPLACE VIEW universe_{key.lower()} AS
            SELECT
                symbol,
                '{key}' AS universe,
                'cn' AS market,
                start_date,
                end_date,
                create_time,
                update_time
            FROM stock_sector
            WHERE sector = '{val}';
        """)

    print("✅ Schema + Views ready")
    
    con.close()

    print("Database initialized!")


if __name__ == "__main__":
    init_db()
