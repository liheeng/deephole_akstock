"""
选股器 Builder — 流式 API，与 PortfolioBuilder 风格一致。

用法:
    picker = (
        StockPickerBuilder
        .new("箱体突破选股")
        .add_pick_strategy("box_breakout")
            .set_strategy_signal("HeavyDrop() & BoxConsolidation() & VolumeBreakout() & PullbackConfirm()")
        .end_strategy()
        .set_pick_op("and")
        .build()
    )
    result = picker.run(data_provider, df)
    symbols = result.get_selected_at(-1)
"""

from __future__ import annotations

from typing import List, Dict, Any
from vectorbt_test.core.signals import Signal
from vectorbt_test.core.factors import Factor
from vectorbt_test.picker.picker_strategy import PickStrategy
from vectorbt_test.picker.picker_portfolio import PickStrategyPortfolio, PickOp


class StockPickerBuilder:
    """
    选股器 Builder — 流式 API。

    支持：
      - 多策略组合（AND / OR / VOTE）
      - 每个策略可设独立信号和因子
      - 可选的全局调仓信号（schedule_signal）
    """

    strategies: List[Dict[str, Any]]
    pick_op: PickOp
    vote_threshold: float
    schedule_signal: str | Signal | Dict[str, str] | None
    _current_strategy: Dict[str, Any] | None

    def __init__(self, name: str):
        self.name = name
        self.strategies = []
        self.pick_op = PickOp.AND
        self.vote_threshold = 0.6
        self.schedule_signal = None
        self._current_strategy = None

    @classmethod
    def new(cls, name: str):
        """创建新的选股器 Builder。"""
        return cls(name)

    # ── 策略管理 ──────────────────────────────────────────────

    def add_pick_strategy(self, name: str):
        """添加一个选股策略。"""
        strategy = {
            "name": name,
            "signal": None,
            "factors": [],
            "threshold": 0.0,
        }
        self.strategies.append(strategy)
        self._current_strategy = strategy
        return self

    def end_strategy(self):
        """结束当前策略的配置。"""
        self._current_strategy = None
        return self

    # ── 当前策略配置 ──────────────────────────────────────────

    def set_strategy_signal(self, signal: str | Signal | Dict[str, str]):
        """设置当前策略的选股信号表达式。"""
        if self._current_strategy:
            self._current_strategy["signal"] = signal
        return self

    def add_factor(self, factor: Factor | str | Dict[str, str]):
        """为当前策略添加排分因子。"""
        if self._current_strategy:
            self._current_strategy["factors"].append(factor)
        return self

    def set_strategy_threshold(self, threshold: float):
        """设置当前策略的阈值（纯因子模式下用）。"""
        if self._current_strategy:
            self._current_strategy["threshold"] = threshold
        return self

    # ── 全局配置 ──────────────────────────────────────────────

    def set_pick_op(self, op: str):
        """设置策略组合方式: 'and' / 'or' / 'vote'。"""
        self.pick_op = PickOp(op.lower() if op else "and")
        return self

    def set_vote_threshold(self, threshold: float):
        """VOTE 模式下，设置通过比例阈值（默认 0.6）。"""
        self.vote_threshold = threshold
        return self

    def set_schedule_signal(self, signal: str | Signal | Dict[str, str]):
        """设置全局调仓信号（仅在这些日期执行选股）。"""
        self.schedule_signal = signal
        return self

    # ── 构建 ──────────────────────────────────────────────────

    def build(self) -> PickStrategyPortfolio:
        """构建选股器。"""
        strategies_obj = []

        for s in self.strategies:
            pick_strat = PickStrategy(
                name=s["name"],
                signal=s["signal"],
                factors=s["factors"] if s["factors"] else None,
                threshold=s["threshold"],
            )
            strategies_obj.append(pick_strat)

        return PickStrategyPortfolio(
            strategies=strategies_obj,
            strategy_op=self.pick_op,
            vote_threshold=self.vote_threshold,
            schedule_signal=self.schedule_signal,
            name=self.name,
        )
