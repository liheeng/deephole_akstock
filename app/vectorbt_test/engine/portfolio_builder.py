from typing import Dict, List, Any
from vectorbt_test.core.factors import Factor
from vectorbt_test.core.portfolio import PortfolioParameters
from vectorbt_test.portfolios.signal_strategy_portfolio import SignalStrategyPortfolio
from vectorbt_test.portfolios.weight_strategy_portfolio import WeightStrategyPortfolio
from vectorbt_test.portfolios.signal_strategy_portfolio import StrategyOp, SignalStrategy
from vectorbt_test.portfolios.weight_strategy_portfolio import WeightStrategy
from vectorbt_test.core.signals import Signal
from vectorbt_test.core.strategy import StrategyMode
from vectorbt_test.core.portfolio import PortfolioType


class PortfolioBuilder:
    strategies: List[Dict[str, Any]] 
    strategy_weights: List[float] | None
    vote_weights: List[float] | None
    strategy_op: StrategyOp
    schedule_signal: str | Signal
    portfolio_params: PortfolioParameters | None
    _current_strategy: Dict[str, Any] | None
    _current_factor: Factor | str
    
    def __init__(self, name: str, portfolio_mode: str):
        self.name = name
        self.portfolio_mode = portfolio_mode
        self.strategies = []
        self.strategy_weights = None
        self.vote_weights = None
        self.strategy_op = StrategyOp.AND
        self.schedule_signal = None
        self.portfolio_params = None

        self._current_strategy = None
        self._current_factor = None

    @classmethod
    def new(cls, name: str, portfolio_mode: str):
        return cls(name, portfolio_mode)
    
    def add_strategy(self, name: str):
        strategy = {
            "name": name,
            "factors": [],
            "signal": None,
            "strategy_mode": None,
            "top_n": None,
            "threshold": None,
        }
        self.strategies.append(strategy)
        self._current_strategy = strategy
        return self
    
    def add_factor(self, factor: Factor | str):
        if self._current_strategy:
            self._current_strategy["factors"].append(factor)
            self._current_factor = factor
        return self

    def end_factor(self):
        self._current_factor = None
        return self
    
    def set_strategy_signal(self, signal: str | Signal):
        if self._current_strategy:
            self._current_strategy['signal'] = signal
        return self
    
    def set_strategy_mode(self, strategy_mode: str | StrategyMode):
        if self._current_strategy:
            self._current_strategy['strategy_mode'] = StrategyMode(strategy_mode) if isinstance(strategy_mode, str) else strategy_mode
        return self
      
    def end_strategy(self):
        self._current_strategy = None
        return self
    
    def set_schedule_signal(self, signal: str | Signal):
        self.schedule_signal = signal
        return self
    
    def set_strategy_op(self, op: str):
        self.strategy_op = StrategyOp(op.lower() if op is not None else StrategyOp.OR)
        return self
    
    def set_strategy_weights(self, weights):
        self.strategy_weights = weights
        return self
    
    def set_vote_weights(self, weights):
        self.vote_weights = weights
        return self

    def set_portfolio_params(self, params: PortfolioParameters):
        self.portfolio_params = params
        return self
    
    def build(self):
        strategies_obj = []

        for s in self.strategies:
            if PortfolioType(self.portfolio_mode.lower()) == PortfolioType.SIGNAL_STRATEGY:
                strategies_obj.append(
                    SignalStrategy(
                        name=s["name"],
                        factors=s["factors"],
                        signal=s["signal"],
                        threshold=float(s["threshold"]) if s["threshold"] is not None else 0.0
                    )
                )
            else:
                strategies_obj.append(
                    WeightStrategy(
                        name=s["name"],
                        factors=s["factors"],
                        signal=s["signal"],
                        top_n=int(s["top_n"]) if s["top_n"] is not None else 10
                    )
                )

        if PortfolioType(self.portfolio_mode.lower()) == PortfolioType.SIGNAL_STRATEGY:
            return SignalStrategyPortfolio(
                strategies=strategies_obj,
                strategy_op=self.strategy_op,
                schedule_signal=self.schedule_signal,
                vote_weights=self.vote_weights,
                portfolio_params=self.portfolio_params
            )
        else:
            return WeightStrategyPortfolio(
                strategies=strategies_obj,
                strategy_weights=self.strategy_weights,
                schedule_signal=self.schedule_signal,
                portfolio_params=self.portfolio_params
            )
