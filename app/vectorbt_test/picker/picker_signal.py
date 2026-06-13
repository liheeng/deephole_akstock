"""
Filter Chain 选股信号基类 — PickerSignal

所有 picker 体系中的 Signal 继承此类而非 TSSignal，
获得统一的 ref_values / set_refs / get_ref 跨阶段上下文传参能力。

filter chain (PickStrategy) 在 evaluate() 前自动调用 signal.set_refs()，
通过 isinstance(signal, PickerSignal) 统一检测，无需 hasattr 分支。
"""

from __future__ import annotations

from typing import Dict
import pandas as pd

from vectorbt_test.core.signals import TSSignal


class PickerSignal(TSSignal):
    """
    Filter Chain 选股信号基类。

    为后阶段信号提供跨阶段上下文传参能力：
      - ref_values: Dict[str, pd.Series]
          存储每只股票的参考值，如 {"high": Series(symbol->值), "low": ..., "volume": ...}
      - set_refs(ref_values):
          由 filter chain (PickStrategy) 在 evaluate() 前自动调用，
          注入上一阶段触发日每只股票的 OHLCV 数据。

    用法:
        class MySignal(PickerSignal):
            def compute(self, data, context):
                ref_high = self.get_ref('high', stock, default=np.inf)
                ...
    """

    def __init__(self, lookback_buffer: int | None = None):
        """
        Args:
            lookback_buffer: from_last 裁剪时在触发日期前额外保留的历史数据天数。
                信号需要回溯窗口时设置此值（如 VolumeBreakout 的 20 日均量计算）。
                None 表示不需要额外回溯，等同于 0。
                filter chain 在裁剪时取 stage.lookback_buffer 和信号自身值的较大值。
        """
        super().__init__()
        self.ref_values: Dict[str, pd.Series] | None = None
        self.lookback_buffer = lookback_buffer if lookback_buffer is not None else 0

    def set_refs(self, ref_values: Dict[str, pd.Series]):
        """由 filter chain 调用，注入 per-stock 参考值。"""
        self.ref_values = ref_values

    def get_ref(self, field: str, stock: str | None = None,
                default: float = 0.0) -> float:
        """
        获取某只股票的参考值。

        Args:
            field: 字段名，如 'high', 'low', 'close', 'volume'
            stock: 股票代码，为 None 时返回 default
            default: 默认值

        Returns:
            参考值，若不存在则返回 default
        """
        if self.ref_values is None or stock is None:
            return default
        series = self.ref_values.get(field)
        if series is None:
            return default
        return series.get(stock, default)
