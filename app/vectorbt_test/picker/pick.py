"""
选股结果定义 — Filter Chain 模式

每个 FilterStage 是一个筛选节点：
  - signal_expr: 信号表达式（可含 {ctx.上阶段名.key} 占位符引用上一阶段输出）
  - time_scope:   "full" / "from_last"
  - params:       可选，占位符替换表

流程：
  全量股票 → Stage1(A&B) → 过滤 → Stage2(C, 引用Stage1.trigger_date) → 过滤 → ...
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any
import pandas as pd


@dataclass
class FilterStage:
    """
    选股筛选节点。

    Attributes:
        name: 节点名称（用于日志/调试，同时作为 ctx 中的 key）
        signal_expr: 信号表达式。
            可含 {ctx.上阶段名.key} 占位符，执行时自动替换为上一阶段的计算值。
            如 "VolumeBreakout(from_date={ctx.大跌.trigger_date})"
        time_scope: 时间范围
            "full"       — 全量时间范围
            "from_last"  — 从上个节点首次触发的时间点开始到现在
        lookback_buffer: time_scope="from_last" 时，在触发日期前额外保留多少天数据。
            用于信号需要回溯窗口的情况（如均量/均线计算），默认 0。
            例如 VolumeBreakout 需要 20 日均量 + 20 日检测窗口 = 40 天回溯。
        params: 额外占位符映射，用于更灵活的上下文引用
            {"from_date": "{ctx.大跌.trigger_date}"}
            执行时会替换 signal_expr 中的所有占位符
    """
    name: str
    signal_expr: str
    time_scope: str = "full"
    lookback_buffer: int = 0
    params: Dict[str, str] | None = None


@dataclass
class StageResult:
    """
    单个筛选节点的执行结果。

    Attributes:
        stage: 节点定义
        signals: 布尔型 DataFrame (dates × 当前剩余股票)
        remaining_symbols: 该节点过滤后剩余的股票代码
        trigger_date: 该节点首次有股票触发的日期
        computed: 该节点计算出的输出值，用于后续阶段的 ctx 引用
            自动包含: trigger_date, stock_count_before, stock_count_after
    """
    stage: FilterStage
    signals: pd.DataFrame | None = None
    remaining_symbols: List[str] | None = None
    trigger_date: pd.Timestamp | None = None
    computed: Dict[str, Any] | None = None


@dataclass
class PickResult:
    """
    选股策略执行结果（Filter Chain 模式）。

    Attributes:
        type: 结果类型，固定为 "pick"
        name: 策略名称
        stage_results: 每个节点的执行结果列表
        remaining_symbols: 最终筛选出的股票代码
        stage_context: 跨阶段上下文，{阶段名: {key: value}}
    """
    type: str = "pick"
    name: str = ""
    stage_results: List[StageResult] | None = None
    remaining_symbols: List[str] | None = None
    stage_context: Dict[str, Dict[str, Any]] | None = None

    @property
    def signals(self) -> pd.DataFrame | None:
        """最终节点（最后一个）的信号矩阵。"""
        if not self.stage_results:
            return None
        return self.stage_results[-1].signals

    def get_selected_at(self, date: str | pd.Timestamp | int = -1) -> List[str]:
        """
        获取最终节点在指定日期被选中的股票。

        Args:
            date: 日期 / -1（最新一天）

        Returns:
            股票代码列表
        """
        if not self.stage_results:
            return []
        last_signals = self.stage_results[-1].signals
        if last_signals is None or last_signals.empty:
            return []
        if isinstance(date, int) and date == -1:
            date = last_signals.index[-1]
        row = last_signals.loc[date]
        return row[row].index.tolist()

    def get_stage_summary(self, stage_idx: int) -> str:
        """获取某个节点的摘要信息。"""
        if not self.stage_results or stage_idx >= len(self.stage_results):
            return ""
        sr = self.stage_results[stage_idx]
        if sr.signals is None or sr.signals.empty:
            remaining = 0
        else:
            remaining = len(sr.remaining_symbols) if sr.remaining_symbols else 0
        trigger = f"触发日: {sr.trigger_date.date()}" if sr.trigger_date else "未触发"
        return f"  Stage [{sr.stage.name}] {sr.stage.signal_expr}: 剩余 {remaining} 只, {trigger}"

    def summary(self) -> str:
        """输出完整可读摘要。"""
        if not self.stage_results:
            return f"[{self.name}] 无结果"
        lines = [f"[{self.name}] Filter Chain 选股结果"]
        for i, sr in enumerate(self.stage_results):
            lines.append(self.get_stage_summary(i))
        final = self.remaining_symbols or []
        lines.append(f"最终入选: {len(final)} 只")
        if final:
            lines.append(f"  股票: {', '.join(final)}")
        return "\n".join(lines)
