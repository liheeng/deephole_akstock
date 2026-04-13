from typing import List, Sequence
import pandas as pd
import vectorbt as vbt
from vectorbt_test.core.portfolio import StrategyPortfolio
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.strategies.weight_strategy import WeightStrategy
from vectorbt_test.core.portfolio import PortfolioParameters
from vectorbt_test.core.signals import Signal
from vectorbt_test.core.strategy import StrategyResult
from vectorbt_test.engine.data_adapter import DataAdapter
from vectorbt_test.engine.data_provider import DataProvider


class WeightStrategyPortfolio(StrategyPortfolio):
    def __init__(self,
                 strategies: Sequence[WeightStrategy],
                 strategy_weights: List[float] | None = None,
                 schedule_signal: str | Signal | None = None,
                 portfolio_params: PortfolioParameters | None = None):
        super().__init__(strategies, schedule_signal, portfolio_params)
        self.strategy_weights = strategy_weights or [1.0 / len(self.strategies)] * len(self.strategies)
        self.freq = portfolio_params.freq if portfolio_params else "1D"
        self.init_cash = portfolio_params.init_cash if portfolio_params else 100000
        
    def run(self, data_provider: DataProvider, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        adapter = DataAdapter(df)
        context = PortfolioContext()
        context.data_provider = data_provider  
        context.data_adapter = adapter

        data = adapter.data
        close = adapter.to_vbt(df["close"])

        # 🔥 bind 一次
        for strat in self.strategies:
            strat.bind_data_adapter(adapter)

        global_schedule = None
        if self.schedule_signal is not None:
            global_schedule = self.schedule_signal.evaluate(data, context)

        # ===== 合成 alpha =====
        alpha = None
        for strat, w in zip(self.strategies, self.strategy_weights):
            results: StrategyResult = strat.generate(data, context)

            if results.type != "weight":
                raise ValueError("Only weight strategies supported")

            alpha = results.weights * w if alpha is None else alpha + results.weights * w

        # 🔥 关键：normalize
        alpha = adapter.cs_normalize(alpha).fillna(0)

        alpha = adapter.to_vbt(alpha).reindex(close.index)

        if global_schedule is not None:
            global_schedule = (
                adapter.to_vbt(global_schedule)
                .reindex(close.index)
                .fillna(False)
            )

            # 🔥 核心：只在调仓日更新
            alpha = alpha.where(global_schedule)

        # 🔥 forward fill → 持仓不变
        alpha = alpha.ffill().fillna(0)

        # ===== delta =====
        delta = alpha - alpha.shift(1).fillna(0)
        min_change = 0.01
        delta = delta.where(delta.abs() > min_change, 0)

        # 🔥 threshold（关键）
        entries = delta > 0
        exits   = delta < 0

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
