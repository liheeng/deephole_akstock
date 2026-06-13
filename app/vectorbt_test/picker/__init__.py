"""
选股框架 (Picker Framework) — Filter Chain 模式

流程：
  全量股票 → Stage1(信号A&B) → 过滤 → Stage2(信号C) → 过滤 → Stage3(信号D|E|F) → 结果

用法：
  from vectorbt_test.picker import StockPickerBuilder, FilterStage, PickResult

  picker = (
      StockPickerBuilder.new("选股器")
      .add_stage("大跌", "HeavyDrop() & BoxConsolidation()")
      .add_stage("突破", "VolumeBreakout()", time_scope="from_last")
      .build()
  )
  result = picker.run(data_provider, df)
  print(result.summary())
"""

from vectorbt_test.picker.pick import PickResult, FilterStage, StageResult
from vectorbt_test.picker.picker_signal import PickerSignal
from vectorbt_test.picker.picker_strategy import PickStrategy
from vectorbt_test.picker.picker_portfolio import PickStrategyPortfolio
from vectorbt_test.picker.picker_builder import StockPickerBuilder

__all__ = [
    "PickResult",
    "FilterStage",
    "StageResult",
    "PickerSignal",
    "PickStrategy",
    "PickStrategyPortfolio",
    "StockPickerBuilder",
]
