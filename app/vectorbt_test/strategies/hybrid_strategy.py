from vectorbt_test.core.factors import Factor, wrap_numberic_node_as_factor
from vectorbt_test.core.signals import Signal, SignalGroup
from vectorbt_test.core.strategy import Strategy, StrategyMode, StrategyResult
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.node_builder import NodeBuilder
from vectorbt_test.core.nodes import NodeType
from typing import List
import numpy as np


class HybridStrategy(Strategy):

    def __init__(
            self, 
            name: str,
            factors: List[Factor | str],
            signal: str | Signal | None = None,
            mode=StrategyMode.AUTO, 
            top_n=10,
            threshold=0):
        """
        Note: 
            |模式 | signal 类型                          |
            | -- | ---------------------------------- |
            | TS | cross / weekly 都行                  |
            | CS | **只能用 schedule（weekly/month_end）** |

        Args:
            factors (List[Factor]): _description_
            signal (str | Signal | None, optional): _description_. Defaults to None.
            mode (_type_, optional): _description_. Defaults to StrategyMode.AUTO.
            top_n (int, optional): _description_. Defaults to 10.
            threshold (int, optional): _description_. Defaults to 0.
        """
        self.name = name
        self.factors: List[Factor] = []
        for f in factors:
            factor: Factor | None = NodeBuilder().build_factor(f, wrap_numberic_node_as_factor) if isinstance(f, str) else f  # type: ignore
            assert factor is not None and factor.type == NodeType.Factor
            self.factors.append(factor)

        self.signal: Signal | None = NodeBuilder().build(signal) if isinstance(signal, str) else signal  # type: ignore
        if self.signal is not None:
            assert self.signal.is_signal

        self.mode = mode
        self.top_n = top_n
        self.threshold = threshold
    
    def generate(self, data, context: PortfolioContext) -> StrategyResult:
        # ===== 1. 合成 alpha =====
        alpha = None
        for f in self.factors:
            s = f.score(data, context)
            alpha = s if alpha is None else alpha + s

        # ===== 2. 判断模式 =====
        mode = self.mode

        if mode == StrategyMode.AUTO:
            mode = StrategyMode.CROSS_SECTION if self.data_adapter.is_cross_section else StrategyMode.TIME_SERIES

        # ===== 3. signal =====
        signal: Signal | None = None
        if self.signal is not None:
            if mode == StrategyMode.TIME_SERIES:
                assert self.signal.is_group(SignalGroup.TS.value | SignalGroup.TS_CS.value)
            else:
                assert self.signal.is_group(SignalGroup.CS.value | SignalGroup.TS_CS.value)

            signal = self.signal.evaluate(data, context)
    
        # ===== 4. 分支 =====
        if mode == StrategyMode.TIME_SERIES:
            return self._ts_strategy(alpha, signal)

        else:
            return self._cs_strategy(alpha, signal)
        
    def _ts_strategy(self, alpha, signal: Signal | None = None) -> StrategyResult:
        entries = alpha > self.threshold
        exits   = alpha < -self.threshold

    # 👇 加 signal gating（核心）
        if signal is not None:
            entries = entries & signal
            exits   = exits & signal

        return StrategyResult(
            type="signal",
            entries=entries,
            exits=exits,
            weights=None
        )
    
    def _cs_strategy(self, alpha, signal: Signal | None = None) -> StrategyResult:
        ranks = self.data_adapter.cs_rank(alpha, ascending=False)

        mask = ranks <= self.top_n

        weights = (self.top_n - ranks + 1).where(mask, 0)
        weights = self.data_adapter.cs_normalize(weights).fillna(0)

        # 👇 核心：只在 signal 时更新权重
        if signal is not None:
            weights = weights.where(signal, np.nan)   # 非调仓日不变

        return StrategyResult(
            type="weight",
            entries=None,
            exits=None,
            weights=weights
        )
