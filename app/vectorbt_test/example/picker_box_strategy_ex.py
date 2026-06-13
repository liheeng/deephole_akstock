"""
箱体突破选股策略 — Filter Chain 5 阶段

流程:
  Stage1: HeavyDrop             — 筑底：最低≥60天前, 最低<最高×0.2, 最高≥120天前
  Stage2: BoxConsolidation      — 盘整：日振幅<2%, 总振幅<10%
  Stage3: VolumeBreakout        — 突破：涨幅>6%, 量>均量×3
  Stage4: ShortBoxConsolidation — 回踩：价在ref高低间, 量<ref量×50%
  Stage5: PullbackConfirm       — 确认：涨幅>5%, 量>ref量×50%, close>ref_close

Stage4/5 通过 ref_values 自动获取 Stage3 触发日的 OHLCV 作为参考值。

使用方式：
  python -m app.vectorbt_test.example.picker_box_strategy_ex
  python -m app.vectorbt_test.example.picker_box_strategy_ex --verbose
"""

import argparse
from db.duckdb import DuckDBController
from db.db_common import DB
from db.stock_daily_util import get_CN_symbols, get_symbols_data

from vectorbt_test.engine.init import load_register_nodes
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.picker import StockPickerBuilder


def main():
    parser = argparse.ArgumentParser(description="箱体突破选股策略")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="打印每阶段剩余股票列表")
    args = parser.parse_args()

    load_register_nodes()
    db_controller = DuckDBController(DB)

    raw_symbols = get_CN_symbols(db_controller)
    symbols = [s[0] if isinstance(s, (list, tuple)) else s for s in raw_symbols]
    symbols_str = ", ".join(symbols)

    start_date = "2022-01-01"
    end_date = "2026-06-11"

    print("=" * 80)
    print("📊 Filter Chain 5 阶段箱体突破选股 — 全市场 A 股")
    print("=" * 80)
    print(f"股票数量: {len(symbols)}  |  时间: {start_date} ~ {end_date}")

    df = get_symbols_data(db_controller, symbols_str, start_date, end_date)
    data_provider = DataProvider(None)
    print(f"数据: {len(df['symbol'].unique())} 只股票, {len(df)} 条记录")

    # ===== Filter Chain =====
    # Stage1-3 独立计算, Stage4/5 自动通过 ref_values 引用 Stage3 的触发日数据
    picker = (
        StockPickerBuilder.new("箱体突破 5 阶段")
        .add_stage("筑底", "HeavyDrop()")
        .add_stage("盘整", "BoxConsolidation()", time_scope="from_last")
        .add_stage("放量突破", "VolumeBreakout()",  time_scope="from_last")
        .add_stage("回踩", "ShortBoxConsolidation()", time_scope="from_last")
        .add_stage("确认", "PullbackConfirm()", time_scope="from_last")
        .build()
    )

    result = picker.run_and_print(data_provider, df, top_n=10, kline_days=60,
                                  verbose=args.verbose)


if __name__ == "__main__":
    main()
