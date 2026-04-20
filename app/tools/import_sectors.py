import duckdb
import pandas as pd
from db.db_common import DB


def load_sector_csv(path="data/stock_sector.csv"):

    con = duckdb.connect(DB)

    df = pd.read_csv(path)

    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    con.register("tmp_sector", df)

    con.execute("""
        INSERT INTO stock_sector
        SELECT 
            symbol,
            sector,
            start_date,
            end_date,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM tmp_sector
    """)

    print("✅ Sector data loaded")