def load_universe_csv(path, universe, market):

    con = duckdb.connect(DB)

    df = pd.read_csv(path)

    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    df["universe"] = universe
    df["market"] = market

    con.register("tmp_uni", df)

    con.execute("""
        INSERT INTO universe_map
        SELECT
            symbol,
            universe,
            market,
            start_date,
            end_date,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM tmp_uni
    """)

    print(f"✅ {universe} loaded")