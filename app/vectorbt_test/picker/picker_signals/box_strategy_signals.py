"""
箱体突破选股策略 — 5 个核心 Signal (v2 Filter Chain)

Filter Chain 流程:
  Stage1: HeavyDrop      — 最低价≥60天前, 最低<最高×0.2, 最高≥120天前
  Stage2: BoxConsolidation — 箱体: 日振幅<2%, 总振幅<10%
  Stage3: VolumeBreakout  — 放量突破: 涨幅>6%, 量>均量×3, 突破箱顶
  Stage4: ShortBoxConsolidation — 短箱体: 价在ref高低间, 量<ref量×50%
  Stage5: PullbackConfirm — 确认: 涨幅>5%, 量>ref量×50%, close>ref_close

所有 Signal 继承 PickerSignal（而非 TSSignal），filter chain 统一通过
isinstance(signal, PickerSignal) 检测并注入 ref_values。
"""

from __future__ import annotations

from enum import Enum
import numpy as np
import pandas as pd

from vectorbt_test.picker.picker_signal import PickerSignal
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from utils.group_func_registry import GroupFuncReg
from vectorbt_test.picker.picker_signals.box_strategy_numba import (
    heavy_drop_numba,
    box_consol_numba_v2, _parse_box_method,
    volume_breakout_numba,
    short_box_consol_numba,
    pullback_confirm_numba_v2,
)


# ====================================================================
#  1. HeavyDropSignal
#  条件:
#    - 最低价距今 ≥ 60 天
#    - 最低价 < 最高价 × 0.2 (跌超 80%)
#    - 最高价距今 ≥ 120 天
# ====================================================================
class HeavyDropSignal(PickerSignal):
    """
    股价从历史高位大幅下跌的筑底信号。

    在滚动窗口内检查:
      1. 最低价 < 最高价 × low_ratio（即跌超 (1-low_ratio)×100%）
      2. 最高价距今 ≥ min_gap_days（确保高位已过去足够久）
      3. 最低价距今 ≥ min_gap_low_days（确保低点已确认）

    Args:
        lookback_days: 回看天数，默认 756（≈ 3 个交易日年）
        min_gap_days: 最高价距今最少天数，默认 120（≈ 6 个月）
        low_ratio: 最低价 / 最高价比率上限，默认 0.2（即跌超 80%）
        min_gap_low_days: 最低价距今最少天数，默认 60（≈ 3 个月）
    """
    def __init__(
        self,
        lookback_days: int = 756,
        min_gap_days: int = 120,
        low_ratio: float = 0.2,
        min_gap_low_days: int = 60,
    ):
        super().__init__()
        self._name = f"HeavyDrop_{lookback_days}_{min_gap_days}_{low_ratio}_{min_gap_low_days}"
        self.lookback_days = lookback_days
        self.min_gap_days = min_gap_days
        self.low_ratio = low_ratio
        self.min_gap_low_days = min_gap_low_days

    def _args(self):
        return [self.lookback_days, self.min_gap_days,
                self.low_ratio, self.min_gap_low_days] + super()._args()

    def compute(self, data, context: PortfolioContext):
        return self.apply(data["close"], self._check, context)

    def _check(self, series):
        if isinstance(series, pd.Series):
            return self._check_single(series)
        result = pd.DataFrame(False, index=series.index, columns=series.columns)
        for col in series.columns:
            result[col] = self._check_single(series[col])
        return result

    def _check_single(self, s: pd.Series) -> pd.Series:
        arr = s.values.astype(np.float64)
        out = heavy_drop_numba(arr, self.lookback_days, self.min_gap_days,
                               self.low_ratio, self.min_gap_low_days)
        return pd.Series(out, index=s.index).cummax()


# ====================================================================
#  2. BoxConsolidationSignal (v2)
#  条件:
#    - 从最低价日到20日前作箱体震荡
#    - 日振幅 (high/low-1) < 2%
#    - 箱体总振幅 < 10%
#    - 支持多种检测方法: range/slope/bandwidth/atr/any/all
# ====================================================================

class BoxMethod(str, Enum):
    """箱体检测方法 — 支持直接用字符串比较 (e.g. BoxMethod.RANGE == "range")"""
    RANGE = "range"
    SLOPE = "slope"
    BANDWIDTH = "bandwidth"
    ATR = "atr"
    ANY = "any"
    ALL = "all"


class BoxConsolidationSignal(PickerSignal):
    """
    箱体震荡检测 — 支持多种检测方法 + 新增日振幅/总振幅约束。

    方法说明：
      range     — 最高价/最低价 ≤ max_range_ratio
      slope     — 线性回归斜率 ≈ 0（|slope|/mean < max_slope_ratio）
      bandwidth — Bollinger 带宽 < max_bandwidth_ratio
      atr       — ATR/close_mean < max_atr_ratio
      any       — 任一方法通过
      all       — 全部方法通过（最严格）

    新增约束（始终应用）：
      - 日振幅 |high/low-1| < max_daily_amp
      - 箱体总振幅 < max_total_range
    """

    def __init__(
        self,
        lookback_days: int = 756,
        recent_days: int = 20,
        # 检测方法与阈值
        box_method: str = "any",
        max_range_ratio: float = 1.30,
        max_slope_ratio: float = 0.001,
        max_bandwidth_ratio: float = 0.15,
        max_atr_ratio: float = 0.03,
        bandwidth_window: int = 20,
        atr_window: int = 14,
        min_valid_points: int = 5,
        # 新增约束
        max_daily_amp: float = 0.03,
        max_total_range: float = 0.20,
    ):
        super().__init__()
        self._name = f"BoxConsol_{lookback_days}_{recent_days}_{box_method}"
        self.lookback_days = lookback_days
        self.recent_days = recent_days
        self.box_method = box_method
        self.max_range_ratio = max_range_ratio
        self.max_slope_ratio = max_slope_ratio
        self.max_bandwidth_ratio = max_bandwidth_ratio
        self.max_atr_ratio = max_atr_ratio
        self.bandwidth_window = bandwidth_window
        self.atr_window = atr_window
        self.min_valid_points = min_valid_points
        self.max_daily_amp = max_daily_amp
        self.max_total_range = max_total_range

    def _args(self):
        return [self.lookback_days, self.recent_days,
                self.box_method, self.max_range_ratio,
                self.max_slope_ratio, self.max_bandwidth_ratio,
                self.max_atr_ratio, self.bandwidth_window,
                self.atr_window, self.min_valid_points,
                self.max_daily_amp, self.max_total_range] + super()._args()

    def compute(self, data, context: PortfolioContext):
        close = data["close"]
        high = data["high"]
        low = data["low"]
        return self._compute_multi(close, high, low)

    def _compute_multi(self, close, high, low):
        if isinstance(close, pd.Series):
            return self._check_single(close, high, low)
        result = pd.DataFrame(False, index=close.index, columns=close.columns)
        box_method_int = _parse_box_method(self.box_method)
        for col in close.columns:
            result[col] = self._check_single(close[col], high[col], low[col], box_method_int)
        return result

    def _check_single(self, close, high, low,
                      box_method_int: int | None = None) -> pd.Series:
        arr_c = close.values.astype(np.float64)
        arr_h = high.values.astype(np.float64)
        arr_l = low.values.astype(np.float64)
        if box_method_int is None:
            box_method_int = _parse_box_method(self.box_method)
        out = box_consol_numba_v2(arr_c, arr_h, arr_l,
                                  self.lookback_days, self.recent_days,
                                  box_method_int,
                                  self.max_range_ratio, self.max_slope_ratio,
                                  self.max_bandwidth_ratio, self.max_atr_ratio,
                                  self.bandwidth_window, self.atr_window,
                                  self.min_valid_points,
                                  self.max_daily_amp, self.max_total_range)
        return pd.Series(out, index=close.index).cummax()


# ====================================================================
#  3. VolumeBreakoutSignal
#  条件:
#    - 20日内首次放量大阳线
#    - 涨幅 > 6%
#    - 成交量 > 前20日均量 × 3
#    - 突破箱体最高价
# ====================================================================
class VolumeBreakoutSignal(PickerSignal):
    """
    放量大阳线突破信号。

    在最近 recent_days 天内检测首次满足以下条件的 K 线:
      1. 涨幅 (close/open - 1) ≥ gain_threshold
      2. 成交量 ≥ 前 vol_ma_days 日均量 × vol_multiple
      3. close 突破前 recent_days 天最高价（箱体顶）
      4. 取最近 recent_days 内的第一根（避免重复触发）
    触发后通过 cummax 锁定信号，后续保持 True。

    lookback_buffer=40: from_last 裁剪时在触发日期前额外保留 40 天数据，
    用于前 vol_ma_days=20 日均量计算和 recent_days=20 检测窗口。

    Args:
        recent_days: 检测窗口天数，默认 20
        gain_threshold: 涨幅阈值，默认 0.06（6%）
        vol_multiple: 成交量倍数阈值，默认 3.0（日均量的 3 倍）
        vol_ma_days: 均量计算天数，默认 20
    """

    def __init__(
        self,
        recent_days: int = 20,
        gain_threshold: float = 0.06,
        vol_multiple: float = 3.0,
        vol_ma_days: int = 20,
):
        # 均量计算需要 vol_ma_days 天历史，检测窗口需要 recent_days 天
        super().__init__(lookback_buffer=40)
        self._name = f"VolBreakout_{recent_days}_{gain_threshold}_{vol_multiple}"
        self.recent_days = recent_days
        self.gain_threshold = gain_threshold
        self.vol_multiple = vol_multiple
        self.vol_ma_days = vol_ma_days

    def _args(self):
        return [self.recent_days, self.gain_threshold,
                self.vol_multiple, self.vol_ma_days] + super()._args()

    def compute(self, data, context: PortfolioContext):
        close = data["close"]
        open_ = data["open"]
        high = data["high"]
        volume = data["volume"]
        return self._compute_multi(close, open_, high, volume)

    def _compute_multi(self, close, open_, high, volume):
        if isinstance(close, pd.Series):
            return self._check_single(close, open_, high, volume)
        result = pd.DataFrame(False, index=close.index, columns=close.columns)
        for col in close.columns:
            result[col] = self._check_single(close[col], open_[col], high[col], volume[col])
        return result

    def _check_single(self, close, open_, high, volume) -> pd.Series:
        arr_c = close.values.astype(np.float64)
        arr_o = open_.values.astype(np.float64)
        arr_h = high.values.astype(np.float64)
        arr_v = volume.values.astype(np.float64)
        out = volume_breakout_numba(arr_c, arr_o, arr_h, arr_v,
                                    self.recent_days, self.gain_threshold,
                                    self.vol_multiple, self.vol_ma_days)
        return pd.Series(out, index=close.index).cummax()


# ====================================================================
#  4. ShortBoxConsolidationSignal (NEW)
#  条件:
#    - 连续2-5日箱体震荡 (从上阶段触发日开始)
#    - 股价在ref_high和ref_low之间
#    - 日成交量 < ref_volume × 50%
#
#  通过 ref_values 从上一阶段获取 per-stock 参考值
# ====================================================================
class ShortBoxConsolidationSignal(PickerSignal):
    """
    短箱体震荡信号 — 在 VolumeBreakout 后的连续窄幅震荡。

    filter chain 自动注入 per-stock ref_values（上一阶段触发日的 OHLCV）:
      - ref_high: 箱体上限（股价必须 ≤ 此值）
      - ref_low: 箱体下限（股价必须 ≥ 此值）
      - ref_volume: 参考成交量（日成交量必须 < 此值 × 50%）

    检查连续 pullback_min ~ pullback_max 天内:
      1. high ≤ ref_high（未突破箱体顶）
      2. low ≥ ref_low（未跌破箱体底）
      3. volume < ref_volume × 50%（缩量盘整）

    Args:
        pullback_min: 最短回调天数，默认 2
        pullback_max: 最长回调天数，默认 5
    """

    def __init__(
        self,
        pullback_min: int = 2,
        pullback_max: int = 5,
    ):
        super().__init__()
        self._name = f"ShortBox_{pullback_min}_{pullback_max}"
        self.pullback_min = pullback_min
        self.pullback_max = pullback_max

    def _args(self):
        return [self.pullback_min, self.pullback_max] + super()._args()

    def compute(self, data, context: PortfolioContext):
        high = data["high"]
        low = data["low"]
        volume = data["volume"]
        return self._compute_multi(high, low, volume)

    def _compute_multi(self, high, low, volume):
        if isinstance(high, pd.Series):
            return self._check_single(high, low, volume)
        result = pd.DataFrame(False, index=high.index, columns=high.columns)
        for col in high.columns:
            result[col] = self._check_single(high[col], low[col], volume[col])
        return result

    def _check_single(self, high, low, volume) -> pd.Series:
        stock = high.name
        ref_h = self.get_ref('high', stock, np.inf)
        ref_l = self.get_ref('low', stock, -np.inf)
        ref_v = self.get_ref('volume', stock, 0.0)

        arr_h = high.values.astype(np.float64)
        arr_l = low.values.astype(np.float64)
        arr_v = volume.values.astype(np.float64)
        out = short_box_consol_numba(arr_h, arr_l, arr_v,
                                     ref_h, ref_l, ref_v,
                                     self.pullback_min, self.pullback_max)
        return pd.Series(out, index=high.index).cummax()


# ====================================================================
#  5. PullbackConfirmSignal (v2)
#  条件:
#    - 再出大阳线: 涨幅 > 5%
#    - 成交量 > ref_volume × 50%
#    - close > ref_close (突破上一阶段收盘价)
#
#  通过 ref_values 从 filter chain 上下文获取
# ====================================================================
class PullbackConfirmSignal(PickerSignal):
    """
    回调确认信号 — 在 ShortBoxConsolidation 后再次放量大阳线。

    filter chain 自动注入 per-stock ref_values（VolumeBreakout 触发日的值）:
      - ref_close: 参考收盘价（确认阳线 close 必须 > 此值）
      - ref_volume: 参考成交量（确认阳线成交量必须 > 此值 × 50%）

    检查:
      1. 涨幅 (close/open - 1) ≥ confirm_gain
      2. 成交量 ≥ ref_volume × 50%
      3. close > ref_close（突破上一阶段收盘价）

    Args:
        confirm_gain: 确认阳线涨幅阈值，默认 0.05（5%）
    """

    def __init__(
        self,
        confirm_gain: float = 0.05,
    ):
        super().__init__()
        self._name = f"PullbackConfirm_{confirm_gain}"
        self.confirm_gain = confirm_gain

    def _args(self):
        return [self.confirm_gain] + super()._args()

    def compute(self, data, context: PortfolioContext):
        close = data["close"]
        open_ = data["open"]
        volume = data["volume"]
        return self._compute_multi(close, open_, volume)

    def _compute_multi(self, close, open_, volume):
        if isinstance(close, pd.Series):
            return self._check_single(close, open_, volume)
        result = pd.DataFrame(False, index=close.index, columns=close.columns)
        for col in close.columns:
            result[col] = self._check_single(close[col], open_[col], volume[col])
        return result

    def _check_single(self, close, open_, volume) -> pd.Series:
        stock = close.name
        ref_c = self.get_ref('close', stock, np.inf)
        ref_v = self.get_ref('volume', stock, 0.0)

        arr_c = close.values.astype(np.float64)
        arr_o = open_.values.astype(np.float64)
        arr_v = volume.values.astype(np.float64)
        out = pullback_confirm_numba_v2(arr_c, arr_o, arr_v,
                                        ref_c, ref_v, self.confirm_gain)
        return pd.Series(out, index=close.index).cummax()


# ====================================================================
#  注册到 NodeRegistry
# ====================================================================
@GroupFuncReg.register(group="nodes")
def register_box_strategy_signals():
    NodeRegistry.register(
        "HeavyDrop",
        lambda lookback_days=756, min_gap_days=120, low_ratio=0.2, min_gap_low_days=60:
            HeavyDropSignal(lookback_days, min_gap_days, low_ratio, min_gap_low_days),
        NodeMeta(name="HeavyDrop", group="signal",
                 desc="最低价距今≥60天, 最低<最高×0.2, 最高距今≥120天",
                 params=[
                     NodeParam("lookback_days", "int", 756, "回看天数"),
                     NodeParam("min_gap_days", "int", 120, "最高距今最少天数"),
                     NodeParam("low_ratio", "float", 0.2, "最低/最高比上限"),
                     NodeParam("min_gap_low_days", "int", 60, "最低距今最少天数"),
                 ]),
    )

    NodeRegistry.register(
        "BoxConsolidation",
        lambda lookback_days=756, recent_days=20, box_method="all",
               max_range_ratio=1.30, max_slope_ratio=0.001,
               max_bandwidth_ratio=0.15, max_atr_ratio=0.03,
               bandwidth_window=20, atr_window=14, min_valid_points=5,
               max_daily_amp=0.02, max_total_range=0.10:
            BoxConsolidationSignal(lookback_days, recent_days, box_method,
                                   max_range_ratio, max_slope_ratio,
                                   max_bandwidth_ratio, max_atr_ratio,
                                   bandwidth_window, atr_window, min_valid_points,
                                   max_daily_amp, max_total_range),
        NodeMeta(name="BoxConsolidation", group="signal",
                 desc="箱体窄幅震荡：日振幅<2%, 总振幅<10%, 支持range/slope/bandwidth/atr多方法",
                 params=[
                     NodeParam("lookback_days", "int", 756, "回看天数"),
                     NodeParam("recent_days", "int", 20, "近期天数"),
                     NodeParam("box_method", "str", "all", "检测方法: range/slope/bandwidth/atr/any/all"),
                     NodeParam("max_range_ratio", "float", 1.30, "range法-最高/最低比上限"),
                     NodeParam("max_slope_ratio", "float", 0.001, "slope法-斜率/均价上限"),
                     NodeParam("max_bandwidth_ratio", "float", 0.15, "bandwidth法-Bollinger带宽上限"),
                     NodeParam("max_atr_ratio", "float", 0.03, "atr法-ATR/均价上限"),
                     NodeParam("bandwidth_window", "int", 20, "bandwidth法-窗口期"),
                     NodeParam("atr_window", "int", 14, "atr法-窗口期"),
                     NodeParam("min_valid_points", "int", 5, "最少有效K线数"),
                     NodeParam("max_daily_amp", "float", 0.02, "日振幅上限"),
                     NodeParam("max_total_range", "float", 0.10, "箱体总振幅上限"),
                 ]),
    )

    NodeRegistry.register(
        "VolumeBreakout",
        lambda recent_days=20, gain_threshold=0.06, vol_multiple=3.0, vol_ma_days=20:
            VolumeBreakoutSignal(recent_days, gain_threshold, vol_multiple, vol_ma_days),
        NodeMeta(name="VolumeBreakout", group="signal",
                 desc="放量突破：涨幅>6%, 量>均量×3, 突破箱顶",
                 params=[
                     NodeParam("recent_days", "int", 20, "近期天数"),
                     NodeParam("gain_threshold", "float", 0.06, "涨幅阈值"),
                     NodeParam("vol_multiple", "float", 3.0, "成交量倍数"),
                     NodeParam("vol_ma_days", "int", 20, "均量计算天数"),
                 ]),
    )

    NodeRegistry.register(
        "ShortBoxConsolidation",
        lambda pullback_min=2, pullback_max=5:
            ShortBoxConsolidationSignal(pullback_min, pullback_max),
        NodeMeta(name="ShortBoxConsolidation", group="signal",
                 desc="短箱体：2-5日价在ref高低间, 量<ref量×50%",
                 params=[
                     NodeParam("pullback_min", "int", 2, "最小回调天数"),
                     NodeParam("pullback_max", "int", 5, "最大回调天数"),
                 ]),
    )

    NodeRegistry.register(
        "PullbackConfirm",
        lambda confirm_gain=0.05:
            PullbackConfirmSignal(confirm_gain),
        NodeMeta(name="PullbackConfirm", group="signal",
                 desc="确认：涨幅>5%, 量>ref量×50%, close>ref_close",
                 params=[
                     NodeParam("confirm_gain", "float", 0.05, "确认阳线涨幅阈值"),
                 ]),
    )
