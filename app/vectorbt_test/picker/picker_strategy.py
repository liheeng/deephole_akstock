"""
选股策略 — 对每只股票独立评估信号，返回布尔掩码。

与 HybridStrategy 的关系：
  - HybridStrategy 负责回测（entry/exit/weight）
  - PickStrategy 负责选股（仅筛选，不交易）

可以共用 Signal 表达式，但输出不同。
"""

from __future__ import annotations

from typing import List, Dict
import pandas as pd

from vectorbt_test.core.strategy import Strategy
from vectorbt_test.picker.pick import PickResult
from vectorbt_test.core.signals import Signal
from vectorbt_test.core.factors import Factor, wrap_numberic_node_as_factor
from vectorbt_test.core.node_builder import NodeBuilder
from vectorbt_test.core.nodes import NodeType
from vectorbt_test.core.context import PortfolioContext


class PickStrategy(Strategy):
    """
    选股策略。

    核心逻辑：
      1. 计算 signal → 布尔掩码（表示该股票当天是否被选中）
      2. 计算 factors → 排分分数（可选，用于排序/权重）
      3. 返回 PickResult

    用法：
      strategy = PickStrategy("my_pick", signal="HeavyDrop() & BoxConsolidation()")
      result: PickResult = strategy.generate(data, context)
      symbols = result.get_selected_at(-1)  # 最新一天被选中的股票
    """

    def __init__(
        self,
        name: str,
        signal: str | Signal | Dict[str, str] | None = None,
        factors: List[Factor | str | Dict[str, str]] | None = None,
        threshold: float = 0.0,
    ):
        self.name = name

        # 解析信号
        self.signal: Signal | None = Signal.build(signal) if signal is not None else None
        if self.signal is not None:
            assert self.signal.is_signal, f"{name}: signal 不是有效的 Signal 类型"

        # 解析因子（可选，用于排分）
        self.factors: List[Factor] = []
        if factors:
            for f in factors:
                factor: Factor | None = NodeBuilder().build_factor(f, wrap_numberic_node_as_factor)  # type: ignore
                assert factor is not None and factor.type == NodeType.Factor  # type: ignore
                self.factors.append(factor)

        self.threshold = threshold

    def generate(self, data, context: PortfolioContext) -> PickResult:
        """
        执行选股。

        Args:
            data: data_view() 返回的 DataView
            context: PortfolioContext

        Returns:
            PickResult
        """
        # ===== 1. 计算信号 =====
        signals: pd.DataFrame | pd.Series | None = None
        if self.signal is not None:
            signals = self.signal.evaluate(data, context)

        # ===== 2. 计算因子分数（可选） =====
        scores: pd.DataFrame | pd.Series | None = None
        if self.factors:
            for f in self.factors:
                s = f.score(data, context)
                scores = s if scores is None else scores + s

        # ===== 3. 纯因子模式：用阈值生成信号 =====
        if signals is None and scores is not None:
            signals = scores > self.threshold

        # ===== 4. 若仍有信号，确保是 DataFrame =====
        if signals is not None and isinstance(signals, pd.Series):
            signals = signals.to_frame(self.name)

        if scores is not None and isinstance(scores, pd.Series):
            scores = scores.to_frame(self.name)

        return PickResult(
            type="pick",
            signals=signals,
            scores=scores,
            name=self.name,
        )
