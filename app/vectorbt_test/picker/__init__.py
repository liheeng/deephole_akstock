"""
选股框架 (Picker Framework)

与回测框架 PortfolioBuilder 体系对应：
  PortfolioBuilder       → StockPickerBuilder
  SignalStrategyPortfolio → PickStrategyPortfolio
  SignalStrategy         → PickStrategy
  PortfolioResultWrapper → PickResult

用法：
  from vectorbt_test.picker import StockPickerBuilder, PickStrategyPortfolio, PickStrategy, PickResult
"""

from vectorbt_test.picker.pick import PickResult
from vectorbt_test.picker.picker_strategy import PickStrategy
from vectorbt_test.picker.picker_portfolio import PickStrategyPortfolio, PickOp
from vectorbt_test.picker.picker_builder import StockPickerBuilder

__all__ = [
    "PickResult",
    "PickStrategy",
    "PickStrategyPortfolio",
    "PickOp",
    "StockPickerBuilder",
]
