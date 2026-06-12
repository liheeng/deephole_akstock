"""
箱体突破选股策略 — 使用 StockPickerBuilder 框架

策略信号组合：
  entry = HeavyDrop & BoxConsolidation & VolumeBreakout & PullbackConfirm

展示 StockPickerBuilder + PickStrategy + PickStrategyPortfolio 的完整用法。

使用方式：
  python -m app.vectorbt_test.example.box_strategy_ex
"""

from db.duckdb import DuckDBController
from db.db_common import DB
from db.stock_daily_util import get_symbols_data

from vectorbt_test.engine.init import load_register_nodes
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.picker import StockPickerBuilder


def main():
    # ===== 1. 注册所有节点 =====
    load_register_nodes()

    # ===== 2. 连接数据库获取数据 =====
    db_controller = DuckDBController(DB)

    symbols_str = "603259.SH, 600362.SH, 000858.SZ, 600519.SH"
    start_date = "2022-01-01"
    end_date = "2026-06-01"

    print("=" * 80)
    print("📊 箱体突破选股策略 (StockPickerBuilder 框架)")
    print("=" * 80)
    print(f"股票池: {symbols_str}")
    print(f"时间范围: {start_date} ~ {end_date}")

    df = get_symbols_data(db_controller, symbols_str, start_date, end_date)
    data_provider = DataProvider(None)

    # ===== 3. 用 StockPickerBuilder 构建选股器 =====
    #
    # 方式 A — 直接在表达式里组合所有 Signal（推荐）
    #   表达式: "HeavyDrop() & BoxConsolidation() & VolumeBreakout() & PullbackConfirm()"
    #
    # 方式 B — 用多策略 OR/AND/VOTE 组合（每个条件单独一个 strategy）
    #   这里演示方式 A

    picker = (
        StockPickerBuilder.new("箱体突破选股")
        .add_pick_strategy("box_breakout")
        .set_strategy_signal(
            "HeavyDrop() & BoxConsolidation() & VolumeBreakout() & PullbackConfirm()"
        )
        .end_strategy()
        .set_pick_op("and")
        .build()
    )

    # ===== 4. 执行选股并打印结果 =====
    result = picker.run_and_print(data_provider, df, top_n=10, kline_days=60)
    _ = result  # PickResult 可用于后续分析

    # 也可以通过 PickResult API 获取数据:
    # symbols = result.get_selected_at(-1)          # 最新一天
    # dates   = result.get_selected_dates()         # 所有触发日期
    # all_sym = result.get_all_selected_symbols()   # 所有被选中过的股票
    # print(result.summary())                       # 可读摘要


if __name__ == "__main__":
    main()
