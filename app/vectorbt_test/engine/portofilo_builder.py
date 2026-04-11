from vectorbt_test.core.factor import Factor
from vectorbt_test.core.signal_expr import BaseExpr
from vectorbt_test.strategy.archieve.factor_strategy import FactorStrategy
from vectorbt_test.strategy.archieve.strategy_portfolio_v2 import StrategyPortfolio, PortfolioParameters
from typing import Dict, List, Any
from .signal_builder import ExprParser


class PortfolioBuilder:
    strategies: List[Dict[str, Any]] 
    strategy_weights: List[float] | None
    portfolio_params: PortfolioParameters | None
    _current_strategy: Dict[str, Any] | None
    _current_factor: Dict[str, Any] | None
    
    def __init__(self, name: str):
        self.name = name
        self.strategies = []
        self.strategy_weights = None
        self.portfolio_params = None

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

    def set_portfolio_params(self, params: PortfolioParameters):
        self.portfolio_params = params
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

        return StrategyPortfolio(
            strategies=strategies_obj,
            strategy_weights=self.strategy_weights,
            portfolio_params=self.portfolio_params
        )
