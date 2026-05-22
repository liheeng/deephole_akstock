import duckdb
import os
from db.db_common import DB
from utils.common import is_running_in_docker

DB_PATH = DB


def update_db():

    data_dir = "/data" if is_running_in_docker() else "./data"
    os.makedirs(data_dir, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    print("🚀 Update database...")

    con.execute("""
        ALTER TABLE stock_indicators ADD COLUMN vol_ma20 DOUBLE;
       
        ALTER TABLE stock_signals ADD COLUMN acc_signal BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN trend_strong BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN trend_weak BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN momentum_strong BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN low_volatility BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN high_volatility BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN volume_spike BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN volume_trend BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN breakout_confirm BOOLEAN;
        ALTER TABLE stock_signals ADD COLUMN reversal_signal BOOLEAN;
    """)

    con.close()
    
    print("✅ Schema + Views ready")

if __name__ == "__main__":
    update_db()