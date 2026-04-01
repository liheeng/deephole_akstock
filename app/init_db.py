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

    # con.execute("""
    # CREATE TABLE IF NOT EXISTS task_log (
    # id BIGINT,
    # task_id TEXT,
    # task_name TEXT,
    # source TEXT,
    # status TEXT,
    # start_time TIMESTAMP,
    # end_time TIMESTAMP,
    # duration DOUBLE,
    # message TEXT)
    # """)
    
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

    con.close()

    print("Database initialized!")


if __name__ == "__main__":
    init_db()
