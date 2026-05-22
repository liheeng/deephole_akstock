"""
============================================================
    统一指标计算入口 — 支持"随时运行，永不重复计算"
============================================================

依赖顺序:
  1. stock_daily (原始数据，由数据同步任务写入)
  2. stock_indicators  ← 依赖 stock_daily（本文件 Step 1）
  3. stock_signals     ← 依赖 stock_indicators（本文件 Step 2）
  4. stock_factor_scores ← 依赖 stock_indicators（本文件 Step 3）

核心原则:
  - 每张目标表都有 PRIMARY KEY (symbol, date)
  - 使用 INSERT OR IGNORE 写入 → 已存在的 (symbol, date) 自动跳过
  - 只在需要时拉取足够的历史数据（保障 rolling 计算精度）
  - 支持按市场（CN / HK / US）更新

用法:
  # 计算所有市场
  python -m app.data_factory.run_all

  # 只更新某个市场
  python -m app.data_factory.run_all --market cn

  # 只更新某个阶段
  python -m app.data_factory.run_all --stage indicators
  python -m app.data_factory.run_all --stage signals
  python -m app.data_factory.run_all --stage factor_scores
"""

import argparse
# import sys
# from pathlib import Path

# # 将项目根目录加入 sys.path（兼容直接执行）
# project_root = Path(__file__).resolve().parent.parent.parent
# if str(project_root) not in sys.path:
#     sys.path.insert(0, str(project_root))

from loguru import logger
from db.db_common import DB
from db.duckdb import DuckDBController
from data_factory.update_indicators import run_indicators
from data_factory.update_signals import run_signals
from data_factory.update_factor_scores import run_factor_scores


def main():
    parser = argparse.ArgumentParser(
        description="统一指标计算 — 随时运行，永不重复计算"
    )
    parser.add_argument(
        "--market",
        "-m",
        type=str,
        default=None,
        choices=["cn", "hk", "us"],
        help="只更新指定市场（默认更新全部）",
    )
    parser.add_argument(
        "--stage",
        "-s",
        type=str,
        default=None,
        choices=["indicators", "signals", "factor_scores"],
        help="只运行指定阶段（默认运行全部三个阶段）",
    )
    args = parser.parse_args()

    logger.info("🚀 启动指标计算")
    logger.info(f"   DB Path: {DB}")
    logger.info(f"   Market:  {args.market or 'ALL'}")
    logger.info(
        f"   Stage:   {args.stage or 'ALL (indicators → signals → factor_scores)'}"
    )

    db = DuckDBController(DB)
   
    stages = [args.stage] if args.stage else ["indicators", "signals", "factor_scores"]

    for stage in stages:
        if stage == "indicators":
            run_indicators(db, args.market)
        elif stage == "signals":
            run_signals(db, args.market)
        # elif stage == "factor_scores":
        #     run_factor_scores(db, args.market)

    logger.info("🎉 全部计算完成！")


if __name__ == "__main__":
    main()
