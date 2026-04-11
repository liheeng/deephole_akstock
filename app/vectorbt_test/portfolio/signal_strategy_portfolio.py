import enum
from typing import List, Sequence
import pandas as pd
import vectorbt as vbt
from ..core.portfolio import PortfolioParameters, StrategyPortfolio
from ..strategy.signal_strategy import SignalStrategy
from vectorbt_test.engine.data_adapter import DataAdapter


class StrategyOp(enum.Enum):
    OR = "or"
    AND = "and"
    VOTE = "vote"


class SignalStrategyPortfolio(StrategyPortfolio):
    def __init__(self,
                 strategies: Sequence[SignalStrategy],
                 strategy_op: StrategyOp = StrategyOp.AND,
                 vote_weights: List[float] | None = None,
                 portfolio_params: PortfolioParameters | None = None):
        super().__init__(strategies, portfolio_params)
        self.strategy_op = strategy_op
        self.vote_weights = vote_weights
        self.freq = self.params.freq if self.params else "1D"
        self.init_cash = self.params.init_cash if self.params else 100000
        
    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        adapter = DataAdapter(df)
        data = adapter.data
        close = adapter.to_vbt(df["close"])
        
        # 🔥 bind 一次
        for strat in self.strategies:
            strat.bind_data_adapter(adapter)

        cache = {}
        entries = None
        exits = None

        if self.strategy_op == StrategyOp.VOTE:
            vote_entries = None
            vote_exits = None

            vote_weights = self.vote_weights if self.vote_weights is not None else [1.0 / len(self.strategies)] * len(self.strategies)
            for strat, w in zip(self.strategies, vote_weights):
                _type, _entries, _exits = strat.generate(data, cache)

                if _type != "signal":
                    raise ValueError("Only signal strategies supported")

                e = _entries.astype(float) * w
                x = _exits.astype(float) * w

                vote_entries = e if vote_entries is None else vote_entries + e
                vote_exits = x if vote_exits is None else vote_exits + x

            # 🔥 可调参数
            entry_threshold = 0.6
            exit_threshold = 0.4

            print("vote_entries mean:", vote_entries.mean())
            print("vote_exits mean:", vote_exits.mean())

            entries = vote_entries >= entry_threshold
            exits = vote_exits >= exit_threshold

        else:
            for strat in self.strategies:
                _type, _entries, _exits = strat.generate(data, cache)

                if _type != "signal":
                    raise ValueError(f"{strat} is not signal strategy")

                if self.strategy_op == StrategyOp.OR:
                    entries = _entries if entries is None else (entries | _entries)
                    exits = _exits if exits is None else (exits | _exits)

                elif self.strategy_op == StrategyOp.AND:
                    entries = _entries if entries is None else (entries & _entries)
                    exits = _exits if exits is None else (exits | _exits)  # 或改成 OR（推荐）
                
        if entries is None or exits is None:
            raise ValueError("No signals generated")

        # 🔥 确保 bool
        entries = entries.astype(bool)
        exits = exits.astype(bool)

        # ===== 转 vectorbt =====
        entries = adapter.to_vbt(entries)
        exits = adapter.to_vbt(exits)

        pf = vbt.Portfolio.from_signals(
            close=close,
            entries=entries,
            exits=exits,
            init_cash=init_cash or self.init_cash,
            freq=freq or self.freq,
        )

        return pf
