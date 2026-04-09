import time
from dataclasses import dataclass
from typing import List
import pandas as pd
import vectorbt as vbt
from vectorbt_test.core.base_strategy import BaseStrategy
from vectorbt_test.engine.signal_engine import SignalEngine
from vectorbt_test.utils.quota_funcs import handle_multi_index


@dataclass
class PortfolioParameters:
    freq: str
    init_cash: float
    buy_threshold: float
    sell_threshold: float
    top_n: int | None
    hold_days: int


class StrategyPortfolio:
    def __init__(self, 
                 strategies: List[BaseStrategy], 
                 strategy_weights: List[float] | None = None, 
                 portfolio_params: PortfolioParameters | None = None):
        self.strategies = strategies
        self.strategy_weights = strategy_weights or [1.0 / len(self.strategies)] * len(self.strategies)
        self.buy_threshold = portfolio_params.buy_threshold if portfolio_params else 0
        self.sell_threshold = portfolio_params.sell_threshold if portfolio_params else 0
        self.freq = portfolio_params.freq if portfolio_params else "1D"
        self.init_cash = portfolio_params.init_cash if portfolio_params else 100000
        self.top_n = portfolio_params.top_n if portfolio_params else None
        self.hold_days = portfolio_params.hold_days if portfolio_params else 1

    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        df, is_multi = handle_multi_index(df)

        alpha = None
        signals_engine = SignalEngine()

        for strat, w in zip(self.strategies, self.strategy_weights):
            score = strat.score(df, signals_engine)
            weighted = score * w
            alpha = weighted if alpha is None else alpha + weighted

        # ===== TopN =====
        if is_multi and self.top_n:
            rank = alpha.groupby(level=0).rank(ascending=False)

            final_entries = rank <= self.top_n
            final_exits = ~(rank <= self.top_n)

            # ===== 持仓周期（强烈推荐）=====
            if getattr(self, "hold_days", 1) > 1:
                for i in range(1, self.hold_days):
                    final_entries |= final_entries.shift(i)

        else:
            final_entries = alpha > self.buy_threshold
            final_exits = alpha < self.sell_threshold

        # ===== 转 vectorbt =====
        def to_vbt(series):
            if isinstance(series.index, pd.MultiIndex):
                df = series.unstack()
                return df.sort_index().ffill()
            return series

        close = to_vbt(df["close"])
        entries = to_vbt(final_entries)
        exits = to_vbt(final_exits)

        pf = vbt.Portfolio.from_signals(
            close=close,
            entries=entries,
            exits=exits,
            init_cash=init_cash or self.init_cash,
            freq=freq or self.freq,
        )

        return pf
