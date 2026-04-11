from typing import List, Sequence
import pandas as pd
import vectorbt as vbt
from ..core.portfolio import StrategyPortfolio
from ..strategy.weight_strategy import WeightStrategy
from ..core.portfolio import PortfolioParameters
from vectorbt_test.engine.data_adapter import DataAdapter


class WeightStrategyPortfolio(StrategyPortfolio):
    def __init__(self,
                 strategies: Sequence[WeightStrategy],
                 strategy_weights: List[float] | None = None,
                 portfolio_params: PortfolioParameters | None = None):
        super().__init__(strategies, portfolio_params)
        self.strategy_weights = strategy_weights or [1.0 / len(self.strategies)] * len(self.strategies)
        self.freq = portfolio_params.freq if portfolio_params else "1D"
        self.init_cash = portfolio_params.init_cash if portfolio_params else 100000
        
    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        adapter = DataAdapter(df)
        data = adapter.data
        close = adapter.to_vbt(df["close"])

        # 🔥 bind 一次
        for strat in self.strategies:
            strat.bind_data_adapter(adapter)

        cache = {}

        # ===== 合成 alpha =====
        alpha = None
        for strat, w in zip(self.strategies, self.strategy_weights):
            _type, weights = strat.generate(data, cache)

            if _type != "weight":
                raise ValueError("Only weight strategies supported")

            alpha = weights * w if alpha is None else alpha + weights * w

        # 🔥 关键：normalize
        alpha = adapter.cs_normalize(alpha).fillna(0)

        # ===== 转 vbt =====
        alpha = adapter.to_vbt(alpha).fillna(0)

        # ===== 对齐 =====
        alpha = alpha.reindex(close.index).fillna(0)

        # ===== rebalance（可选）=====
        if self.portfolio_params and self.portfolio_params.hold_days > 1:
            alpha.iloc[::self.portfolio_params.hold_days] = alpha.iloc[::self.portfolio_params.hold_days]
            alpha = alpha.ffill()

        # ===== delta =====
        delta = alpha - alpha.shift(1).fillna(0)

        # 🔥 threshold（关键）
        threshold = 0.001

        entries = delta > threshold
        exits   = delta < -threshold

        size = delta.where(entries | exits, 0).abs()

        pf = vbt.Portfolio.from_signals(
            close=close,
            entries=entries,
            exits=exits,
            size=size,
            size_type="percent",
            init_cash=init_cash or self.init_cash,
            freq=freq or self.freq,
        )

        return pf
