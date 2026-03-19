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
    
    con.close()

    print("Database initialized!")


if __name__ == "__main__":
    init_db()
