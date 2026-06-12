"""
箱体突破选股策略 — 四个核心 Signal

条件拆解：
  条件 1&2 → HeavyDropSignal   : 股价从 3 年内高位跌超 60%，且高位距今 ≥ 6 个月
  条件 3   → BoxConsolidationSignal : 从最低点至今 20 日内做箱体窄幅震荡
  条件 4   → VolumeBreakoutSignal   : 20 日内首次放量大阳线突破箱体顶
  条件 5&6 → PullbackConfirmSignal  : 突破后回调 2-5 日不破首阳收盘，再出阳线创新高
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import linregress
from enum import Enum

from vectorbt_test.core.signals import TSSignal
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from utils.group_func_registry import GroupFuncReg


# ====================================================================
#  条件 1 & 2 : HeavyDropSignal
#  股价已大幅下跌，现价低于最近高位价格的 40% (即跌超 60%)
#  历史（3 年内）最近高位价格与现在至少间隔半年以上
# ====================================================================
class HeavyDropSignal(TSSignal):
    """
    检测股价从历史高位大幅下跌的信号。

    对每只股票，在滚动 lookback_days（默认 756 ≈ 3 年）窗口内:
    1. 找窗口内最高价（不含今日）
    2. 检查今日收盘价 < drop_ratio × 最高价（默认 0.4，即跌超 60%）
    3. 检查最高价距今 ≥ min_gap_days（默认 126 ≈ 半年）
    """

    def __init__(
        self,
        lookback_days: int = 756,
        drop_ratio: float = 0.4,
        min_gap_days: int = 126,
    ):
        super().__init__()
        self._name = f"HeavyDrop_{lookback_days}_{drop_ratio}_{min_gap_days}"
        self.lookback_days = lookback_days
        self.drop_ratio = drop_ratio
        self.min_gap_days = min_gap_days

    def _args(self):
        return [self.lookback_days, self.drop_ratio, self.min_gap_days] + super()._args()

    def compute(self, data, context: PortfolioContext):
        return self.apply(data["close"], self._check, context)

    def _check(self, series: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
        # ---- Series 分支（单标）-----------------------------------------------
        if isinstance(series, pd.Series):
            return self._check_single(series)

        # ---- DataFrame 分支（多标）--------------------------------------------
        result = pd.DataFrame(False, index=series.index, columns=series.columns)
        for col in series.columns:
            result[col] = self._check_single(series[col])
        return result

    def _check_single(self, s: pd.Series) -> pd.Series:
        out = pd.Series(False, index=s.index)
        arr = s.values.astype(np.float64)
        n = len(arr)
        lb = self.lookback_days   # 最多回看天数（cap）
        gap = self.min_gap_days   # 最少间隔天数（实际最低数据要求）

        for i in range(gap, n):
            # 窗口 [max(0,i-lb), i-1] — 动态大小，至多 lb 天
            win_start = max(0, i - lb)
            window = arr[win_start : i]
            win_len = len(window)
            current = arr[i]
            if np.isnan(current):
                continue

            max_val = np.nanmax(window)
            if max_val <= 0 or np.isnan(max_val):
                continue

            # (1) 跌幅足够
            if current >= max_val * self.drop_ratio:
                continue

            # (2) 最高价距今天数
            max_pos_in_window = win_len - 1 - np.nanargmax(window[::-1])
            days_since_max = win_len - max_pos_in_window

            if days_since_max >= gap:
                out.iloc[i] = True

        # 累积锁存：一旦触发，保持 TRUE 到当前
        return out.cummax()


# ====================================================================
#  条件 3 : BoxConsolidationSignal
#  股价企稳，在距当前日期 20 日到 N 日（N = 最高价跌下后的最低价时间点）
#  的时间内在一定价格区间内做箱体窄幅震荡，每日振幅在合理范围
# ====================================================================
class BoxMethod(str, Enum):
    """箱体检测方法"""
    RANGE = "range"           # 价格区间比率
    SLOPE = "slope"           # 线性回归斜率
    BANDWIDTH = "bandwidth"   # Bollinger 带宽
    ATR = "atr"               # ATR 比率
    ANY = "any"               # 任一方法通过即可
    ALL = "all"               # 全部方法需同时通过


class BoxConsolidationSignal(TSSignal):
    """
    箱体震荡检测 — 支持多种检测方法。

    方法说明：
      range     — 最高价/最低价 ≤ max_range_ratio
      slope     — 线性回归斜率 ≈ 0（|slope|/mean < max_slope_ratio）
      bandwidth — Bollinger 带宽 < max_bandwidth_ratio（窄带 = 盘整）
      atr       — ATR/close_mean < max_atr_ratio（低波动 = 盘整）
      any       — 任一方法通过
      all       — 全部方法通过（最严格）
    """

    def __init__(
        self,
        lookback_days: int = 756,
        drop_ratio: float = 0.4,
        recent_days: int = 20,
        # 检测方法与阈值
        box_method: str = "all",
        max_range_ratio: float = 1.30,
        max_slope_ratio: float = 0.001,
        max_bandwidth_ratio: float = 0.15,
        max_atr_ratio: float = 0.03,
        bandwidth_window: int = 20,
        atr_window: int = 14,
        min_valid_points: int = 5,
    ):
        super().__init__()
        self._name = f"BoxConsol_{lookback_days}_{drop_ratio}_{recent_days}_{box_method}"
        self.lookback_days = lookback_days
        self.drop_ratio = drop_ratio
        self.recent_days = recent_days
        self.box_method = box_method
        self.max_range_ratio = max_range_ratio
        self.max_slope_ratio = max_slope_ratio
        self.max_bandwidth_ratio = max_bandwidth_ratio
        self.max_atr_ratio = max_atr_ratio
        self.bandwidth_window = bandwidth_window
        self.atr_window = atr_window
        self.min_valid_points = min_valid_points

    def _args(self):
        return [self.lookback_days, self.drop_ratio, self.recent_days,
                self.box_method, self.max_range_ratio, self.max_slope_ratio,
                self.max_bandwidth_ratio, self.max_atr_ratio,
                self.bandwidth_window, self.atr_window,
                self.min_valid_points] + super()._args()

    def compute(self, data, context: PortfolioContext):
        close = data["close"]
        high = data["high"]
        low = data["low"]
        return self._compute_multi(close, high, low)

    def _compute_multi(self, close, high, low):
        if isinstance(close, pd.Series):
            return self._check_single(close, high, low)

        result = pd.DataFrame(False, index=close.index, columns=close.columns)
        for col in close.columns:
            result[col] = self._check_single(
                close[col], high[col], low[col]
            )
        return result

    # =========================
    # 各检测方法
    # =========================

    @staticmethod
    def _check_range(box_h: np.ndarray, box_l: np.ndarray,
                     max_ratio: float) -> bool:
        """方法 1: 价格区间比率"""
        max_h = np.nanmax(box_h)
        min_l = np.nanmin(box_l)
        if max_h <= 0 or min_l <= 0:
            return False
        return (max_h / min_l) <= max_ratio

    @staticmethod
    def _check_slope(box_c: np.ndarray, max_ratio: float) -> bool:
        """方法 2: 线性回归斜率"""
        valid = ~np.isnan(box_c)
        if valid.sum() < 5:
            return False
        y = box_c[valid]
        x = np.arange(len(y))
        try:
            slope, _, r_value, _, _ = linregress(x, y)
        except Exception:
            return False
        mean_price = np.mean(y)
        if mean_price <= 0:
            return False
        # 斜率接近零 + R² 不高（说明不是强趋势）
        return abs(slope) / mean_price <= max_ratio

    def _check_bandwidth(self, box_c: np.ndarray, max_ratio: float) -> bool:
        """方法 3: Bollinger 带宽 — 带宽越小越盘整"""
        valid = ~np.isnan(box_c)
        if valid.sum() < self.bandwidth_window:
            return False
        y = box_c[valid][-self.bandwidth_window:]
        mean = np.mean(y)
        std = np.std(y, ddof=1)
        if mean <= 0:
            return False
        bandwidth = 2 * std / mean  # 2 倍标准差带宽
        return bandwidth <= max_ratio

    @staticmethod
    def _check_atr(box_h: np.ndarray, box_l: np.ndarray, box_c: np.ndarray,
                   window: int, max_ratio: float) -> bool:
        """方法 4: ATR 比率 — ATR 低 = 低波动盘整"""
        n = len(box_c)
        if n < window + 1:
            return False

        # 计算 ATR
        tr = np.full(n, np.nan)
        for i in range(1, n):
            hl = box_h[i] - box_l[i]
            hc = abs(box_h[i] - box_c[i - 1])
            lc = abs(box_l[i] - box_c[i - 1])
            tr[i] = np.nanmax([hl, hc, lc])

        # 取最近的 ATR（滚动均值）
        recent_tr = tr[-window:]
        valid_tr = recent_tr[~np.isnan(recent_tr)]
        if len(valid_tr) < window // 2:
            return False

        atr = np.mean(valid_tr)
        mean_close = np.nanmean(box_c[-window:])
        if mean_close <= 0:
            return False

        return (atr / mean_close) <= max_ratio

    # =========================
    # 主检测逻辑
    # =========================

    def _check_single(self, close: pd.Series, high: pd.Series, low: pd.Series) -> pd.Series:
        out = pd.Series(False, index=close.index)
        arr_c = close.values.astype(np.float64)
        arr_h = high.values.astype(np.float64)
        arr_l = low.values.astype(np.float64)
        n = len(arr_c)
        lb = self.lookback_days
        recent = self.recent_days
        method = self.box_method

        for i in range(recent, n):
            win_start = max(0, i - lb)

            current = arr_c[i]
            if np.isnan(current):
                continue

            # ---- 最高价检查（同 HeavyDrop）----
            hist_window = arr_c[win_start : i]
            max_val = np.nanmax(hist_window)
            if max_val <= 0 or np.isnan(max_val):
                continue
            if current >= max_val * self.drop_ratio:
                continue

            # ---- 找箱体起点：首次跌幅达标日 ----
            hist_len = len(hist_window)
            max_pos = hist_len - 1 - np.nanargmax(hist_window[::-1])
            max_abs_idx = win_start + max_pos

            box_start: int | None = None
            for j in range(max_abs_idx, i + 1):
                if arr_c[j] < max_val * self.drop_ratio:
                    box_start = j
                    break
            if box_start is None:
                continue

            # ---- 箱体窗口 ----
            box_end = i - recent
            if box_start >= box_end:
                continue

            box_c = arr_c[box_start : box_end + 1]
            box_h = arr_h[box_start : box_end + 1]
            box_l = arr_l[box_start : box_end + 1]

            # 过滤 NaN
            valid = ~(np.isnan(box_h) | np.isnan(box_l) | np.isnan(box_c))
            if valid.sum() < self.min_valid_points:
                continue

            box_c_v = box_c[valid]
            box_h_v = box_h[valid]
            box_l_v = box_l[valid]

            # ---- 各方法打分 ----
            pass_range = self._check_range(box_h_v, box_l_v, self.max_range_ratio)
            pass_slope = self._check_slope(box_c_v, self.max_slope_ratio)
            pass_bw = self._check_bandwidth(box_c_v, self.max_bandwidth_ratio)
            pass_atr = self._check_atr(box_h_v, box_l_v, box_c_v,
                                       self.atr_window, self.max_atr_ratio)

            # ---- 根据 method 组合结果 ----
            if method == "range":
                ok = pass_range
            elif method == "slope":
                ok = pass_slope
            elif method == "bandwidth":
                ok = pass_bw
            elif method == "atr":
                ok = pass_atr
            elif method == "any":
                ok = pass_range or pass_slope or pass_bw or pass_atr
            else:  # "all"
                ok = pass_range and pass_slope and pass_bw and pass_atr

            if ok:
                out.iloc[i] = True

        # 累积锁存：一旦箱体确认，保持 TRUE 到当前
        return out.cummax()


# ====================================================================
#  条件 4 : VolumeBreakoutSignal
#  近期（二十日内）首次出现放量大阳线，突破箱体最高价，
#  且涨幅 ≥ 6%，成交量是前面 20 日均量的 3 倍以上
# ====================================================================
class VolumeBreakoutSignal(TSSignal):
    """
    放量大阳线突破信号。

    检查最近 recent_days（默认 20）天内：
    1. 当日涨幅 (= close/open - 1) ≥ gain_threshold（默认 0.06）
    2. 当日成交量 ≥ vol_multiple × 前 20 日均量（默认 3 倍）
    3. close 价格突破箱体最高价（用前面 20 天高点作为箱体顶）
    4. 取最近 recent_days 内符合条件的"第一根"K 线
    """

    def __init__(
        self,
        recent_days: int = 20,
        gain_threshold: float = 0.06,
        vol_multiple: float = 3.0,
        vol_ma_days: int = 20,
    ):
        super().__init__()
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
            result[col] = self._check_single(
                close[col], open_[col], high[col], volume[col]
            )
        return result

    def _check_single(
        self,
        close: pd.Series,
        open_: pd.Series,
        high: pd.Series,
        volume: pd.Series,
    ) -> pd.Series:
        out = pd.Series(False, index=close.index)
        arr_c = close.values.astype(np.float64)
        arr_o = open_.values.astype(np.float64)
        arr_h = high.values.astype(np.float64)
        arr_v = volume.values.astype(np.float64)

        n = len(arr_c)
        lookback = self.recent_days
        vol_ma = self.vol_ma_days

        for i in range(lookback + vol_ma, n):
            current_close = arr_c[i]
            current_open = arr_o[i]
            current_high = arr_h[i]
            current_vol = arr_v[i]

            if np.isnan(current_close) or np.isnan(current_open):
                continue
            if current_open <= 0:
                continue

            # (1) 涨幅 ≥ gain_threshold
            gain = current_close / current_open - 1.0
            if gain < self.gain_threshold:
                continue

            # (2) 成交量检查
            vol_window = arr_v[i - vol_ma : i]
            avg_vol = np.nanmean(vol_window)
            if avg_vol <= 0 or current_vol < avg_vol * self.vol_multiple:
                continue

            # (3) 突破箱体顶：检查最近 lookback 天的高点
            high_window = arr_h[i - lookback : i]
            box_top = np.nanmax(high_window)
            if current_close <= box_top:
                continue

            # (4) 是最近 lookback 天内的"第一次"突破
            #    往前看 lookback 天，不能有已经满足条件的
            already_break = False
            for j in range(i - lookback, i):
                if out.iloc[j]:
                    already_break = True
                    break
            if already_break:
                continue

            out.iloc[i] = True

        # 累积锁存：一旦突破，保持 TRUE 到当前
        return out.cummax()


# ====================================================================
#  条件 5 & 6 : PullbackConfirmSignal
#  突破后回调 2-5 日，未跌破首根大阳线的收盘价
#  此后，再次出现一根大阳线，close 突破首根大阳线的最高价
# ====================================================================
class PullbackConfirmSignal(TSSignal):
    """
    突破后回调确认信号。

    流程：
    1. 先用 VolumeBreakoutSignal 找到"首根大阳线"的位置
    2. 对大阳线后的 2-5 个交易日检查：
       - 最低价始终 ≥ 首阳收盘价 × pullback_floor_ratio（默认 0.98，允许微幅跌破）
    3. 在回调后的任意一天，出现确认阳线：
       - close / open - 1 ≥ confirm_gain（默认 0.04，次阳可略低于首阳)
       - close > 首阳最高价
    4. 信号在该确认日触发
    """

    def __init__(
        self,
        pullback_min: int = 2,
        pullback_max: int = 5,
        pullback_floor_ratio: float = 0.98,
        confirm_gain: float = 0.04,
        # 传给 VolumeBreakoutSignal 的参数
        breakout_recent_days: int = 20,
        breakout_gain: float = 0.06,
        breakout_vol_multiple: float = 3.0,
        breakout_vol_ma_days: int = 20,
    ):
        super().__init__()
        self._name = f"PullbackConfirm_{pullback_min}_{pullback_max}"
        self.pullback_min = pullback_min
        self.pullback_max = pullback_max
        self.pullback_floor_ratio = pullback_floor_ratio
        self.confirm_gain = confirm_gain

        # 内部持有 VolumeBreakoutSignal 实例
        self._breakout_signal = VolumeBreakoutSignal(
            recent_days=breakout_recent_days,
            gain_threshold=breakout_gain,
            vol_multiple=breakout_vol_multiple,
            vol_ma_days=breakout_vol_ma_days,
        )

    def _args(self):
        return [self.pullback_min, self.pullback_max, self.pullback_floor_ratio,
                self.confirm_gain] + super()._args()

    def compute(self, data, context: PortfolioContext):
        close = data["close"]
        open_ = data["open"]
        high = data["high"]
        low = data["low"]
        volume = data["volume"]
        return self._compute_multi(close, open_, high, low, volume)

    def _compute_multi(self, close, open_, high, low, volume):
        if isinstance(close, pd.Series):
            return self._check_single(close, open_, high, low, volume)

        result = pd.DataFrame(False, index=close.index, columns=close.columns)
        for col in close.columns:
            result[col] = self._check_single(
                close[col], open_[col], high[col], low[col], volume[col]
            )
        return result

    def _check_single(
        self,
        close: pd.Series,
        open_: pd.Series,
        high: pd.Series,
        low: pd.Series,
        volume: pd.Series,
    ) -> pd.Series:
        # ---- 先计算首阳位置 ------------------------------------------------
        breakout = self._breakout_signal._check_single(close, open_, high, volume)
        breakout_idx = np.where(breakout.values)[0]

        if len(breakout_idx) == 0:
            return pd.Series(False, index=close.index)

        arr_c = close.values.astype(np.float64)
        arr_o = open_.values.astype(np.float64)
        arr_h = high.values.astype(np.float64)
        arr_l = low.values.astype(np.float64)
        n = len(arr_c)

        out = pd.Series(False, index=close.index)

        for bi in breakout_idx:
            first_close = arr_c[bi]
            first_high = arr_h[bi]
            if np.isnan(first_close) or np.isnan(first_high):
                continue

            # ---- 从 pullback_min 天后开始找确认阳线 -------------------------
            for ci in range(bi + self.pullback_min, n):
                # 跳过无效数据
                if np.isnan(arr_c[ci]) or np.isnan(arr_o[ci]) or arr_o[ci] <= 0:
                    continue

                # 检查 ci 是否是确认阳线
                gain = arr_c[ci] / arr_o[ci] - 1.0
                if gain < self.confirm_gain or arr_c[ci] <= first_high:
                    continue

                # ---- 验证已发生的回调期 [bi+1, ci-1] 未跌破地板 ------------
                pullback_days = ci - bi - 1

                # 回调不能超过 pullback_max（否则换下一个首阳）
                if pullback_days > self.pullback_max:
                    break

                # 逐日检查（只检查已经发生的日子，无前视偏差）
                pullback_ok = True
                for pi in range(bi + 1, ci):
                    if not np.isnan(arr_l[pi]) and arr_l[pi] < first_close * self.pullback_floor_ratio:
                        pullback_ok = False
                        break

                if not pullback_ok:
                    continue  # 这个确认不成立，继续找下一个 ci

                # ✅ 全部条件满足
                out.iloc[ci] = True
                break  # 只取第一次确认

        # 累积锁存：一旦确认，保持 TRUE 到当前
        return out.cummax()


# ====================================================================
#  注册到 NodeRegistry
# ====================================================================
@GroupFuncReg.register(group="nodes")
def register_box_strategy_signals():
    NodeRegistry.register(
        "HeavyDrop",
        lambda
            lookback_days=756,
            drop_ratio=0.4,
            min_gap_days=126: HeavyDropSignal(lookback_days, drop_ratio, min_gap_days),
        NodeMeta(
            name="HeavyDrop",
            group="signal",
            desc="股价从 3 年内高位跌超 60%，且高位距今 ≥ 半年",
            params=[
                NodeParam("lookback_days", "int", 756, "回看天数（默认 756 ≈ 3 年）"),
                NodeParam("drop_ratio", "float", 0.4, "跌幅比例（默认 0.4，即现价 < 高位×0.4）"),
                NodeParam("min_gap_days", "int", 126, "高位距今最少天数（默认 126 ≈ 半年）"),
            ],
        ),
    )

    NodeRegistry.register(
        "BoxConsolidation",
        lambda
            lookback_days=756,
            drop_ratio=0.4,
            recent_days=20,
            box_method="all",
            max_range_ratio=1.30,
            max_slope_ratio=0.001,
            max_bandwidth_ratio=0.15,
            max_atr_ratio=0.03,
            bandwidth_window=20,
            atr_window=14,
            min_valid_points=5: BoxConsolidationSignal(
                lookback_days, drop_ratio, recent_days,
                box_method, max_range_ratio, max_slope_ratio,
                max_bandwidth_ratio, max_atr_ratio,
                bandwidth_window, atr_window, min_valid_points
            ),
        NodeMeta(
            name="BoxConsolidation",
            group="signal",
            desc="箱体窄幅震荡，支持 range/slope/bandwidth/atr 多种检测方法",
            params=[
                NodeParam("lookback_days", "int", 756, "回看天数"),
                NodeParam("drop_ratio", "float", 0.4, "跌幅比例"),
                NodeParam("recent_days", "int", 20, "近期天数"),
                NodeParam("box_method", "str", "all",
                          "检测方法: range/slope/bandwidth/atr/any/all"),
                NodeParam("max_range_ratio", "float", 1.30, "range 法-最高/最低比上限"),
                NodeParam("max_slope_ratio", "float", 0.001, "slope 法-斜率/均价上限"),
                NodeParam("max_bandwidth_ratio", "float", 0.15, "bandwidth 法-Bollinger带宽上限"),
                NodeParam("max_atr_ratio", "float", 0.03, "atr 法-ATR/均价上限"),
                NodeParam("bandwidth_window", "int", 20, "bandwidth 法-窗口期"),
                NodeParam("atr_window", "int", 14, "atr 法-窗口期"),
                NodeParam("min_valid_points", "int", 5, "箱体窗口最少有效K线数"),
            ],
        ),
    )

    NodeRegistry.register(
        "VolumeBreakout",
        lambda
            recent_days=20,
            gain_threshold=0.06,
            vol_multiple=3.0,
            vol_ma_days=20: VolumeBreakoutSignal(
                recent_days, gain_threshold, vol_multiple, vol_ma_days
            ),
        NodeMeta(
            name="VolumeBreakout",
            group="signal",
            desc="20 日内首次放量大阳线突破：涨幅 ≥ 6%，量 ≥ 20 日均量×3",
            params=[
                NodeParam("recent_days", "int", 20, "近期天数"),
                NodeParam("gain_threshold", "float", 0.06, "涨幅阈值"),
                NodeParam("vol_multiple", "float", 3.0, "成交量倍数"),
                NodeParam("vol_ma_days", "int", 20, "均量计算天数"),
            ],
        ),
    )

    NodeRegistry.register(
        "PullbackConfirm",
        lambda
            pullback_min=2,
            pullback_max=5,
            pullback_floor_ratio=0.98,
            confirm_gain=0.04,
            breakout_recent_days=20,
            breakout_gain=0.06,
            breakout_vol_multiple=3.0,
            breakout_vol_ma_days=20: PullbackConfirmSignal(
                pullback_min, pullback_max, pullback_floor_ratio,
                confirm_gain, breakout_recent_days, breakout_gain,
                breakout_vol_multiple, breakout_vol_ma_days,
            ),
        NodeMeta(
            name="PullbackConfirm",
            group="signal",
            desc="突破后回调 2-5 日不破首阳收盘，再出阳线创新高确认",
            params=[
                NodeParam("pullback_min", "int", 2, "最小回调天数"),
                NodeParam("pullback_max", "int", 5, "最大回调天数"),
                NodeParam("pullback_floor_ratio", "float", 0.98,
                          "回调最低价 / 首阳收盘价 比率下限"),
                NodeParam("confirm_gain", "float", 0.04, "确认阳线涨幅阈值"),
                NodeParam("breakout_recent_days", "int", 20, "首阳判断-近期天数"),
                NodeParam("breakout_gain", "float", 0.06, "首阳判断-涨幅阈值"),
                NodeParam("breakout_vol_multiple", "float", 3.0, "首阳判断-量倍数"),
                NodeParam("breakout_vol_ma_days", "int", 20, "首阳判断-均量计算天数"),
            ],
        ),
    )
