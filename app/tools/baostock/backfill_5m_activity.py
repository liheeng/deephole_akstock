"""
全表回填 kline_5minutes 的 3 个指标字段：
  - amplitude_pct  = (high - low) / NULLIF(open, 0) * 100
  - amount_log     = LN(NULLIF(amount, 0) + 1)
  - activity_bias  = (close - open) / NULLIF(high - low, 0)

策略：逐只股票更新，单只股票事务提交，内存开销极低。
RK3399 4G 内存 + 3000万行数据可用。

用法: python backfill_5m_activity.py
"""

import duckdb
import os
from loguru import logger
from utils.common import is_running_in_docker
from tqdm import tqdm

BAOSTOCK_HIS_DB_PATH = os.environ.get(
    "BAOSTOCK_HIS_DB_PATH", "/data" if is_running_in_docker() else "./data"
)
DB_PATH = BAOSTOCK_HIS_DB_PATH + "/baostock_data.duckdb"

logger.add(
    "./logs/backfill_5m_activity.log", rotation="100 MB", encoding="utf-8", enqueue=True
)


def ensure_columns(con):
    """确保目标列存在"""
    existing_cols = [
        row[1] for row in con.execute("PRAGMA table_info(kline_5minutes)").fetchall()
    ]
    target_cols = ["amplitude_pct", "amount_log", "activity_bias"]
    for col in target_cols:
        if col not in existing_cols:
            logger.info(f"列 {col} 不存在，正在添加...")
            con.execute(f"ALTER TABLE kline_5minutes ADD COLUMN {col} DOUBLE;")


def main():
    con = duckdb.connect(DB_PATH)
    ensure_columns(con)

    # 获取所有需要更新的股票代码
    codes = [
        row[0]
        for row in con.execute("""
            SELECT DISTINCT code FROM kline_5minutes
            WHERE amplitude_pct IS NULL
               OR amount_log IS NULL
               OR activity_bias IS NULL
            ORDER BY code
        """).fetchall()
    ]

    if not codes:
        logger.info("✅ 全部已更新，无需回填")
        con.close()
        return

    logger.info(f"需要更新的股票数: {len(codes)}")

    for code in tqdm(codes, desc="回填进度"):
        # 每只股票独立事务，避免大事务撑爆内存
        con.execute("BEGIN TRANSACTION;")
        try:
            con.execute("""
                UPDATE kline_5minutes
                SET
                    amplitude_pct = ((high - low) / NULLIF(open, 0) * 100),
                    amount_log    = LN(NULLIF(amount, 0) + 1),
                    activity_bias = (close - open) / NULLIF(high - low, 0)
                WHERE code = ?
                  AND (amplitude_pct IS NULL
                    OR amount_log IS NULL
                    OR activity_bias IS NULL)
            """, [code])
            con.execute("COMMIT;")
            logger.info(f"✅ {code} 更新完成")
        except Exception:
            con.execute("ROLLBACK;")
            logger.exception(f"❌ {code} 更新失败，跳过")
            continue

    logger.success(f"🎉 回填完成")

    # 验证
    remaining = con.execute("""
        SELECT COUNT(*) FROM kline_5minutes
        WHERE amplitude_pct IS NULL OR amount_log IS NULL OR activity_bias IS NULL
    """).fetchone()[0]
    logger.info(f"剩余未更新行数: {remaining}")
    con.close()


if __name__ == "__main__":
    main()
