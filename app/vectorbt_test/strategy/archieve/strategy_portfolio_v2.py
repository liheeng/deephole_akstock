from dataclasses import dataclass
from typing import List
import pandas as pd
import vectorbt as vbt
from vectorbt_test.core.strategy import Strategy
from vectorbt_test.utils.quota_funcs import adjust_data_index


def orthogonalize_factors(factors: List[pd.DataFrame]):
    ortho = []

    for f in factors:
        f_new = f.copy()

        for prev in ortho:
            # 投影
            beta = (f_new * prev).sum(axis=1) / (prev * prev).sum(axis=1)
            beta = beta.replace([float("inf"), -float("inf")], 0).fillna(0)

            f_new = f_new - prev.mul(beta, axis=0)

        ortho.append(f_new)

    return ortho

@dataclass
class PortfolioParameters:
    freq: str
    init_cash: float
    top_n: int | None
    hold_days: int


class StrategyPortfolioV2:
    def __init__(self, 
                 strategies: List[Strategy], 
                 strategy_weights: List[float] | None = None, 
                 portfolio_params: PortfolioParameters | None = None):
        self.strategies = strategies
        self.strategy_weights = strategy_weights or [1.0 / len(self.strategies)] * len(self.strategies)
        self.freq = portfolio_params.freq if portfolio_params else "1D"
        self.init_cash = portfolio_params.init_cash if portfolio_params else 100000
        self.top_n = portfolio_params.top_n if portfolio_params else None
        self.hold_days = portfolio_params.hold_days if portfolio_params else 1

    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        df, is_multi = adjust_data_index(df)

        alpha = None
        cache = {}

        # ===== 合成 alpha =====
        for strat, w in zip(self.strategies, self.strategy_weights):
            score = strat.generate(df, cache)
            alpha = score * w if alpha is None else alpha + score * w

        weights = alpha

        # ===== 调仓频率（rebalance）=====
        if self.hold_days > 1:
            weights = weights.copy()
            weights.iloc[::self.hold_days] = weights.iloc[::self.hold_days]
            weights = weights.ffill()

        # ===== 转 vectorbt =====
        def to_vbt(series):
            if isinstance(series.index, pd.MultiIndex):
                df = series.unstack()
                return df.sort_index().ffill()
            return series

        close = to_vbt(df["close"])
        weights = to_vbt(weights).fillna(0)

        # 计算变化量（核心）
        delta = weights - weights.shift(1).fillna(0)

        threshold = 0.01 # 1%
        entries = delta > threshold
        exits = delta < -threshold

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
