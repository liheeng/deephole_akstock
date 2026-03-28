import duckdb
import os
import shutil
import time
import glob

DB_PATH = "data/stock.duckdb"
OUTPUT_DIR = "cn_stock_csv"

start = time.time()
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect(DB_PATH)
con.execute("PRAGMA threads=8;")

# 1. 导出
con.execute(f"""
COPY (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        amount,
        pct,
        turnover
    FROM stock_daily
    WHERE market = 'CN'
    ORDER BY symbol, date
)
TO '{OUTPUT_DIR}'
(FORMAT 'csv', HEADER, PARTITION_BY (symbol));
""")

con.close()

# 2. 合并 + 重命名
for name in os.listdir(OUTPUT_DIR):
    dir_path = os.path.join(OUTPUT_DIR, name)

    if os.path.isdir(dir_path) and name.startswith("symbol="):
        symbol = name.split("=")[1]
        dst_file = os.path.join(OUTPUT_DIR, f"{symbol}.csv")

        csv_files = sorted(glob.glob(os.path.join(dir_path, "data_*.csv")))

        with open(dst_file, "w") as outfile:
            for i, f in enumerate(csv_files):
                with open(f, "r") as infile:
                    if i != 0:
                        next(infile)  # 跳过header
                    outfile.write(infile.read())

        shutil.rmtree(dir_path)

print("完成")
print("耗时:", time.time() - start)