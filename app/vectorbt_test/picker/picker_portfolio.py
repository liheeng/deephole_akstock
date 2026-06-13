"""
选股策略组合 — Filter Chain 模式。

将单个 PickStrategy（含多阶段 FilterStage）作为策略执行，
输出 PickResult。
"""

from __future__ import annotations

import pandas as pd

from vectorbt_test.picker.pick import PickResult
from vectorbt_test.picker.picker_strategy import PickStrategy
from vectorbt_test.engine.data_provider import DataProvider


class PickStrategyPortfolio:
    """
    选股策略组合器（Filter Chain 模式）。

    持有单个 PickStrategy（内含多个 FilterStage），
    按顺序执行 filter chain 筛选。
    """

    def __init__(
        self,
        strategy: PickStrategy,
        name: str = "picker",
    ):
        self.strategy = strategy
        self.name = name

    def run(self, data_provider: DataProvider, df: pd.DataFrame) -> PickResult:
        """
        执行 Filter Chain 选股。

        Args:
            data_provider: DataProvider
            df: 原始长格式 DataFrame

        Returns:
            PickResult
        """
        return self.strategy.generate(df, data_provider)

    def run_and_print(
        self,
        data_provider: DataProvider,
        df: pd.DataFrame,
        top_n: int = 10,
        kline_days: int = 60,
        verbose: bool = False,
    ) -> PickResult:
        """
        执行选股并打印完整结果。

        Args:
            data_provider: DataProvider
            df: 原始数据
            top_n: 显示排分前 N 的股票（0 为全部）
            kline_days: K线显示的近期天数
            verbose: 为 True 时每阶段打印剩余股票列表

        Returns:
            PickResult
        """
        result = self.run(data_provider, df)

        return self.print_result(df, top_n, kline_days, result, verbose=verbose)

    def print_result(self, df, top_n, kline_days, result, verbose=False):
        print("=" * 80)
        print(f"📊 Filter Chain 选股策略: {self.name}")
        print("=" * 80)

        # ── 每阶段摘要 ─────────────────────────────────────
        if result.stage_results:
            for i, sr in enumerate(result.stage_results):
                # 用 computed.stock_count_before 作为"之前"数量
                # （不能直接用 signals.columns 计数，因为 from_last 裁剪可能使部分股票在信号矩阵中消失）
                n_before = sr.computed.get("stock_count_before", 0) if sr.computed else 0
                n_after = len(sr.remaining_symbols) if sr.remaining_symbols else 0
                scope = sr.stage.time_scope
                trigger = f"触发@{sr.trigger_date.date()}" if sr.trigger_date else "未触发"
                cutoff = sr.computed.get("stage_cutoff") if sr.computed else None
                cutoff_str = ""
                if cutoff is not None:
                    try:
                        cutoff_str = f"  截断@{cutoff.date()}"
                    except AttributeError:
                        cutoff_str = f"  截断@{pd.Timestamp(cutoff).date()}"
                print(f"\n  Stage {i+1}: [{sr.stage.name}] {sr.stage.signal_expr}")
                print(f"    时间范围: {scope}{cutoff_str}  |  之前 {n_before} 只 → 之后 {n_after} 只  |  {trigger}")
                if verbose and sr.remaining_symbols:
                    # 每行 10 只分列打印
                    syms = sr.remaining_symbols
                    for chunk_start in range(0, len(syms), 10):
                        chunk = syms[chunk_start:chunk_start + 10]
                        print(f"      {'  '.join(chunk)}")

        # ── 最终结果 ─────────────────────────────────────────
        final = result.remaining_symbols or []
        print(f"\n{'=' * 80}")
        print(f"✅ 最终入选: {len(final)} 只")
        if final:
            print(f"   股票列表: {', '.join(final)}")

            # 输出 K 线数据
            for sym in final[:top_n] if top_n > 0 else final:
                sym_data = df[df['symbol'] == sym].copy()
                if sym_data.empty:
                    continue
                latest_date = sym_data['date'].max()
                recent = sym_data[sym_data['date'] >= latest_date - pd.Timedelta(days=kline_days)].tail(30)

                print(f"\n  {sym} 近期 K 线:")
                print(f"  {'日期':<12s} {'开盘':>8s} {'最高':>8s} {'最低':>8s} {'收盘':>8s} {'成交量':>12s}")
                print(f"  {'-' * 56}")
                for _, row in recent.iterrows():
                    print(f"  {str(row['date'])[:10]:<12s} {row['open']:>8.2f} {row['high']:>8.2f} "
                          f"{row['low']:>8.2f} {row['close']:>8.2f} {row['volume']:>12.0f}")
        else:
            print("   无满足条件的股票")

        return result
