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

    con.close()
    
    print("✅ Schema + Views ready")

if __name__ == "__main__":
    update_db()