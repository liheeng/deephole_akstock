from vectorbt_test.core.factor import Factor
from vectorbt_test.core.signal_expr import BaseExpr
from vectorbt_test.strategy.factor_strategy import FactorStrategy
from vectorbt_test.strategy.strategy_portfolio import StrategyPortfolio
from typing import Dict, List, Any
from .signal_builder import ExprParser


class PortfolioBuilder:
    strategies: List[Dict[str, Any]] 
    strategy_weights: List[float] | None
    run_params: Dict[str, Any]
    _current_strategy: Dict[str, Any] | None
    _current_factor: Dict[str, Any] | None
    
    def __init__(self, name: str):
        self.name = name
        self.strategies = []
        self.strategy_weights = None
        self.run_params = {}

        self._current_strategy = None
        self._current_factor = None

    @classmethod
    def new(cls, name: str):
        return cls(name)
    
    def add_strategy(self, name: str):
        strategy = {
            "name": name,
            "factors": []
        }
        self.strategies.append(strategy)
        self._current_strategy = strategy
        return self
    
    def add_factor(self, name: str):
        factor = {
            "name": name,
            "expr": None
        }
        if self._current_strategy:
            self._current_strategy["factors"].append(factor)
            self._current_factor = factor
        return self
    
    def add_expr(self, expr: BaseExpr | str):
        _expr: BaseExpr | str | Any
        if isinstance(expr, str):
            parser = ExprParser()
            _expr = parser.parse(expr)
        else:
            _expr = expr

        if self._current_factor:
            self._current_factor["expr"] = _expr
        return self

    def end_factor(self):
        self._current_factor = None
        return self

    def end_strategy(self):
        self._current_strategy = None
        return self
    
    def set_strategy_weights(self, weights):
        self.strategy_weights = weights
        return self

    def set_run_parameters(self, **kwargs):
        self.run_params = kwargs
        return self
    
    def build(self):
        strategies_obj = []

        for s in self.strategies:
            factors_obj = []

            for f in s["factors"]:
                factors_obj.append(
                    Factor(name=f["name"], expr=f["expr"])
                )

            strategies_obj.append(
                FactorStrategy(
                    name=s["name"],
                    factors=factors_obj
                )
            )

        freq = self.run_params.get("freq", "1D")
        init_cash = self.run_params.get("init_cash", 100000)

        return StrategyPortfolio(
            strategies=strategies_obj,
            strategy_weights=self.strategy_weights,
            freq=freq,
            init_cash=init_cash
        )
