"""
选股结果定义

与 backtest 体系中的 StrategyResult / PortfolioResultWrapper 类似，
但专注选股场景：输出满足条件的股票代码及排序分数。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple
import pandas as pd


@dataclass
class PickResult:
    """
    选股策略执行结果。

    Attributes:
        type: 结果类型，固定为 "pick"
        signals: 布尔型 DataFrame (dates × symbols)，True 表示该日该股被选中
        scores: 可选，浮点型 DataFrame (dates × symbols)，排分/因子分数
        name: 策略名称
    """
    type: str = "pick"
    signals: pd.DataFrame | None = None
    scores: pd.DataFrame | None = None
    name: str = ""

    def get_selected_at(self, date: str | pd.Timestamp | int = -1) -> List[str]:
        """
        获取指定日期被选中的股票代码。

        Args:
            date: 日期字符串 / Timestamp / -1（最新一天）

        Returns:
            股票代码列表
        """
        if self.signals is None or self.signals.empty:
            return []
        if isinstance(date, int) and date == -1:
            date = self.signals.index[-1]
        row = self.signals.loc[date]
        return row[row].index.tolist()

    def get_selected_dates(self) -> List[Tuple[pd.Timestamp, List[str]]]:
        """
        获取所有有选中记录的日期及对应股票列表（从最新日期开始）。

        Returns:
            [(date, [symbol, ...]), ...]
        """
        if self.signals is None or self.signals.empty:
            return []
        result = []
        for date in reversed(self.signals.index):
            symbols = self.get_selected_at(date)
            if symbols:
                result.append((date, symbols))
        return result

    def get_all_selected_symbols(self) -> List[str]:
        """获取所有历史被选中过的股票代码（去重）。"""
        if self.signals is None or self.signals.empty:
            return []
        return self.signals.columns[self.signals.any()].tolist()

    def summary(self) -> str:
        """输出可读摘要。"""
        if self.signals is None or self.signals.empty:
            return f"[{self.name}] 无选中记录"
        dates = self.get_selected_dates()
        if not dates:
            return f"[{self.name}] 无选中记录"
        lines = [f"[{self.name}] 共 {len(dates)} 个触发日期"]
        for date, symbols in dates[:5]:
            sym_str = ', '.join(symbols[:5])
            suffix = '...' if len(symbols) > 5 else ''
            lines.append(f"  📅 {date.date()}: {len(symbols)} 只 → {sym_str}{suffix}")
        if len(dates) > 5:
            lines.append(f"  ... 还有 {len(dates) - 5} 个日期")
        return "\n".join(lines)
