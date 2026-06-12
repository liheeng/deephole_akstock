"""
选股策略组合 — 支持 AND / OR / VOTE 多种组合方式。

与 SignalStrategyPortfolio 类似，但输出为 PickResult 而非 vectorbt Portfolio。
"""

from __future__ import annotations

import enum
from typing import List, Sequence, Dict
import pandas as pd

from vectorbt_test.picker.pick import PickResult
from vectorbt_test.core.signals import Signal
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.engine.context_builder import create_context


class PickOp(enum.Enum):
    """选股组合操作符"""
    OR = "or"        # 任一策略选中即入选
    AND = "and"      # 所有策略均选中才入选
    VOTE = "vote"    # 投票机制，超过阈值入选


class PickStrategyPortfolio:
    """
    选股策略组合器。

    将多个 PickStrategy 的结果按指定方式合并，输出最终的 PickResult。
    """

    def __init__(
        self,
        strategies: Sequence,
        strategy_op: PickOp = PickOp.AND,
        vote_threshold: float = 0.6,
        schedule_signal: str | Signal | Dict[str, str] | None = None,
        name: str = "picker",
    ):
        self.strategies = strategies
        self.strategy_op = strategy_op
        self.vote_threshold = vote_threshold
        self.schedule_signal: Signal | None = Signal.build(schedule_signal) if schedule_signal is not None else None
        self.name = name

    def run(self, data_provider: DataProvider, df: pd.DataFrame) -> PickResult:
        """
        执行选股组合。

        Args:
            data_provider: DataProvider 实例
            df: 原始长格式 DataFrame（必须包含 symbol、date 等列）

        Returns:
            合并后的 PickResult
        """
        context = create_context(df, data_provider)
        adapter = context.data_adapter
        data = adapter.data_view()

        # bind data_adapter 到每个策略
        for strat in self.strategies:
            strat.bind_data_adapter(adapter)

        # ===== 全局调仓信号 =====
        global_schedule = None
        if self.schedule_signal is not None:
            global_schedule = self.schedule_signal.evaluate(data, context)

        # ===== 执行各策略 =====
        results: List[PickResult] = []
        for strat in self.strategies:
            result: PickResult = strat.generate(data, context)
            results.append(result)

        # ===== 合并信号 =====
        combined_signals = self._combine_signals(results, global_schedule)

        # ===== 合并分数 =====
        combined_scores = self._combine_scores(results)

        return PickResult(
            type="pick",
            signals=combined_signals,
            scores=combined_scores,
            name=self.name,
        )

    def _combine_signals(
        self, results: List[PickResult],
        global_schedule: pd.DataFrame | pd.Series | None = None
    ) -> pd.DataFrame | None:
        """合并多个策略的信号掩码。"""
        # 过滤出有信号的结果
        valid = [r for r in results if r.signals is not None and not r.signals.empty]
        if not valid:
            return None

        if self.strategy_op == PickOp.OR:
            combined = None
            for r in valid:
                combined = r.signals if combined is None else (combined | r.signals)
        elif self.strategy_op == PickOp.AND:
            combined = None
            for r in valid:
                combined = r.signals if combined is None else (combined & r.signals)
        elif self.strategy_op == PickOp.VOTE:
            total = len(valid)
            vote_sum = None
            for r in valid:
                vote_sum = r.signals.astype(float) if vote_sum is None else vote_sum + r.signals.astype(float)
            combined = vote_sum >= (total * self.vote_threshold)
        else:
            raise ValueError(f"不支持的组合操作: {self.strategy_op}")

        if combined is None:
            return None

        # 应用全局调仓信号
        if global_schedule is not None:
            combined = combined & global_schedule

        return combined.astype(bool)

    def _combine_scores(self, results: List[PickResult]) -> pd.DataFrame | None:
        """合并多个策略的分数（简单加和）。"""
        valid = [r for r in results if r.scores is not None and not r.scores.empty]
        if not valid:
            return None

        combined = None
        for r in valid:
            combined = r.scores if combined is None else combined + r.scores
        return combined

    def run_and_print(
        self,
        data_provider: DataProvider,
        df: pd.DataFrame,
        top_n: int = 10,
        kline_days: int = 60,
    ) -> PickResult:
        """
        执行选股并打印结果。

        Args:
            data_provider: DataProvider
            df: 原始数据
            top_n: 仅显示排分前 N 的股票（0 为全部）
            kline_days: K线显示的近期天数

        Returns:
            PickResult
        """
        result = self.run(data_provider, df)

        print("=" * 80)
        print(f"📊 选股策略: {self.name}")
        print(f"组合方式: {self.strategy_op.value}")
        print("=" * 80)

        if result.signals is None or result.signals.empty:
            print("\n❌ 未发现满足条件的股票")
            return result

        print(f"\n信号矩阵: {result.signals.shape[0]} 天 × {result.signals.shape[1]} 只股票")

        # 查找所有触发日期
        trigger_dates = result.get_selected_dates()
        if not trigger_dates:
            print("\n❌ 未发现满足条件的股票")
            return result

        print(f"\n✅ 共 {len(trigger_dates)} 个触发日期")

        for date, symbols in trigger_dates:
            print(f"\n{'=' * 80}")
            print(f"📅 触发日期: {date.date()}")

            # 若有排分，按分数排序
            if result.scores is not None and date in result.scores.index:
                day_scores = result.scores.loc[date]
                sorted_symbols = day_scores.dropna().sort_values(ascending=False)
                symbols = [s for s in sorted_symbols.index if s in symbols]
                print(f"排分前 {top_n}: {', '.join(symbols[:top_n])}")

            print(f"股票数量: {len(symbols)}")
            if top_n > 0:
                print(f"股票列表: {', '.join(symbols[:top_n])}{'...' if len(symbols) > top_n else ''}")
            else:
                print(f"股票列表: {', '.join(symbols)}")

            # 输出 K 线数据
            for sym in symbols[:top_n] if top_n > 0 else symbols:
                sym_data = df[df['symbol'] == sym].copy()
                recent = sym_data[sym_data['date'] >= pd.Timestamp(date) - pd.Timedelta(days=kline_days)].tail(30)

                print(f"\n  {sym} 近期 K 线:")
                print(f"  {'日期':<12s} {'开盘':>8s} {'最高':>8s} {'最低':>8s} {'收盘':>8s} {'成交量':>12s}")
                print(f"  {'-' * 56}")
                for _, row in recent.iterrows():
                    print(f"  {str(row['date'])[:10]:<12s} {row['open']:>8.2f} {row['high']:>8.2f} "
                          f"{row['low']:>8.2f} {row['close']:>8.2f} {row['volume']:>12.0f}")

        return result
