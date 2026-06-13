"""
选股器 Builder — Filter Chain 流式 API，支持跨阶段上下文引用。

用法:
    # 基本用法
    picker = (
        StockPickerBuilder.new("箱体突破选股")
        .add_stage("大跌筑底", "HeavyDrop() & BoxConsolidation()")
        .add_stage("放量突破", "VolumeBreakout()", time_scope="from_last")
        .build()
    )

    # 引用上一阶段计算值
    picker = (
        StockPickerBuilder.new("高级选股")
        .add_stage("大跌", "HeavyDrop() & BoxConsolidation()")
        .add_stage("突破",
            "VolumeBreakout(from_date={ctx.大跌.trigger_date})",
            time_scope="from_last")
        .build()
    )
"""

from __future__ import annotations

from typing import List, Dict
from vectorbt_test.picker.pick import FilterStage
from vectorbt_test.picker.picker_strategy import PickStrategy
from vectorbt_test.picker.picker_portfolio import PickStrategyPortfolio


class StockPickerBuilder:
    """
    选股器 Builder — Filter Chain 流式 API。

    通过 add_stage() 添加筛选节点，形成 filter chain。
    支持 {ctx.阶段名.key} 占位符引用上一阶段的计算值。
    """

    def __init__(self, name: str):
        self.name = name
        self.stages: List[FilterStage] = []

    @classmethod
    def new(cls, name: str):
        """创建新的选股器 Builder。"""
        return cls(name)

    def add_stage(self, name: str, signal_expr: str,
                  time_scope: str = "full",
                  lookback_buffer: int = 0,
                  params: Dict[str, str] | None = None):
        """
        添加一个筛选节点。

        Args:
            name: 节点名称（如 "大跌筑底"），也是 ctx 中的 key
            signal_expr: 信号表达式。
                可含 {ctx.上阶段名.key} 占位符，运行时替换为实际值。
                如 "VolumeBreakout(from_date={ctx.大跌.trigger_date})"
            time_scope: 时间范围
                "full"       — 全量时间范围
                "from_last"  — 从上个节点首次触发的时间点开始
            lookback_buffer: time_scope="from_last" 时，在触发日期前额外保留的数据天数。
                用于需要回溯窗口的信号（如均量计算、移动均线等）。
                例如 VolumeBreakout 需要 20 日均量 + 20 日检测窗口 = 40。
            params: 额外占位符映射
                {"from_date": "{ctx.大跌.trigger_date}"}

        Returns:
            self
        """
        self.stages.append(FilterStage(
            name=name,
            signal_expr=signal_expr,
            time_scope=time_scope,
            lookback_buffer=lookback_buffer,
            params=params,
        ))
        return self

    def build(self) -> PickStrategyPortfolio:
        """构建选股器。"""
        strategy = PickStrategy(
            name=self.name,
            stages=self.stages,
        )
        return PickStrategyPortfolio(
            strategy=strategy,
            name=self.name,
        )
