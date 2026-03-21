import duckdb
import os
from db.db_common import DB
DB_PATH = DB

def init_db():

    os.makedirs("/data", exist_ok=True)

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
        symbol VARCHAR,
        market VARCHAR,
        date DATE,

        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,

        volume DOUBLE,
        amount DOUBLE,

        pct DOUBLE,
        turnover DOUBLE,

        update_time TIMESTAMP DEFAULT now()
    )
    """)

    con.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uniq_symbol_date
    ON stock_daily(symbol, date)
    """)

    con.execute("""
    CREATE INDEX idx_market_date
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
    CREATE TABLE tasks (
        task_id TEXT,
        status TEXT,
        source TEXT,
        start_time TIMESTAMP,
        end_time TIMESTAMP
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS task_log (
    id BIGINT,
    task_id TEXT,
    task_name TEXT,
    source TEXT,
    status TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration DOUBLE,
    message TEXT)
    """)
    
    con.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR PRIMARY KEY,
    desc VARCHAR,
    status VARCHAR,
    mode VARCHAR,

    start_time TIMESTAMP,
    execute_time TIMESTAMP,
    stop_time TIMESTAMP,

    message TEXT,

    create_time TIMESTAMP DEFAULT now(),            
    update_time TIMESTAMP DEFAULT now())
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR PRIMARY KEY,
    type VARCHAR,
    status VARCHAR,
    task_id VARCHAR,
                
    params JSON,
    depends_on VARCHAR,   -- JSON string
                
    retries INTEGER,
    retry_count INTEGER,

    execute_time TIMESTAMP,
    stop_time TIMESTAMP,

    message TEXT,
    error TEXT,
                
    create_time TIMESTAMP DEFAULT now(),            
    update_time TIMESTAMP DEFAULT now())
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS running_jobs (
        job_id TEXT PRIMARY KEY,
        task_id TEXT,
        type TEXT,
        concurrency_key TEXT,
        start_time TIMESTAMP
                
        create_time TIMESTAMP DEFAULT now(),            
        update_time TIMESTAMP DEFAULT now())
    )
    """)

    con.execute("""
    CREATE UNIQUE INDEX idx_running_unique
    ON running_jobs(concurrency_key, job_id)
    """)

    con.close()

    print("Database initialized!")


if __name__ == "__main__":
    init_db()
