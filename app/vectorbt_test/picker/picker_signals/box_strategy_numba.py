"""
箱体突破选股策略 — Numba 加速内核 (v2 Filter Chain)

5 个 Signal 的 numba 内核：
  1. heavy_drop_numba       — 最低价 < 最高×0.2, 最低≥60天, 最高≥120天
  2. box_consol_numba_v2    — 箱体：日振幅<2%, 总振幅<10%
  3. volume_breakout_numba  — 放量突破：涨幅>6%, 量>20日均量×3
  4. short_box_consol_numba — 短箱体：价在ref高低间, 量<ref量×50%
  5. pullback_confirm_numba_v2 — 确认：涨幅>5%, 量>ref量×50%, close>ref_close
"""

from numba import njit
import numpy as np


# ====================================================================
#  1. HeavyDropSignal
#  条件：最低价≥60天前, 最低<最高×0.2, 最高≥120天前
# ====================================================================

@njit(cache=True)
def heavy_drop_numba(arr: np.ndarray, lookback_days: int,
                     min_gap_days: int = 120,
                     low_ratio: float = 0.2,
                     min_gap_low_days: int = 60) -> np.ndarray:
    n = len(arr)
    out = np.zeros(n, dtype=np.bool_)

    for i in range(min_gap_days, n):
        win_start = max(0, i - lookback_days)
        if np.isnan(arr[i]):
            continue

        max_val = -np.inf
        max_pos = 0
        min_val = np.inf
        min_pos = 0
        for j in range(win_start, i):
            v = arr[j]
            if v > max_val:
                max_val = v
                max_pos = j
            if v < min_val:
                min_val = v
                min_pos = j

        if max_val <= 0 or np.isnan(max_val):
            continue

        # (1) 最低价 < 最高价 × low_ratio (default 0.2, 即跌80%)
        if min_val >= max_val * low_ratio:
            continue

        # (2) 最高价距今 ≥ min_gap_days (default 120)
        days_since_max = i - max_pos
        if days_since_max < min_gap_days:
            continue

        # (3) 最低价距今 ≥ min_gap_low_days (default 60)
        days_since_low = i - min_pos
        if days_since_low >= min_gap_low_days:
            out[i] = True

    return out


# ====================================================================
#  2. BoxConsolidationSignal (v2)
#  条件：箱体日振幅<3%, 总振幅<10%, 窗口从最低价日到20日前,
#        支持多种检测方法: range/slope/bandwidth/atr/any/all
# ====================================================================

# 方法枚举常量（int，numba 兼容）
_RANGE = 0
_SLOPE = 1
_BANDWIDTH = 2
_ATR = 3
_ANY = 4
_ALL = 5


def _parse_box_method(method: str) -> int:
    """将字符串方法名转为整数常量。"""
    return {
        "range": _RANGE, "slope": _SLOPE, "bandwidth": _BANDWIDTH,
        "atr": _ATR, "any": _ANY, "all": _ALL,
    }.get(method, _ALL)


@njit(cache=True)
def _ols_slope(y: np.ndarray) -> float:
    """简单 OLS 斜率（numba 兼容）。"""
    n = len(y)
    if n < 2:
        return 0.0
    x = np.arange(n, dtype=np.float64)
    x_mean = np.mean(x)
    y_mean = np.mean(y)
    num = np.sum((x - x_mean) * (y - y_mean))
    den = np.sum((x - x_mean) ** 2)
    return num / den if den != 0 else 0.0


@njit(cache=True)
def _check_range_numba(box_h: np.ndarray, box_l: np.ndarray,
                       max_ratio: float) -> bool:
    """方法: 价格区间比率"""
    max_h = np.nanmax(box_h)
    min_l = np.nanmin(box_l)
    if max_h <= 0 or min_l <= 0:
        return False
    return (max_h / min_l) <= max_ratio


@njit(cache=True)
def _check_slope_numba(box_c: np.ndarray, max_ratio: float) -> bool:
    """方法: 线性回归斜率"""
    valid = ~np.isnan(box_c)
    if valid.sum() < 5:
        return False
    y = box_c[valid]
    mean_price = np.mean(y)
    if mean_price <= 0:
        return False
    slope = _ols_slope(y)
    return abs(slope) / mean_price <= max_ratio


@njit(cache=True)
def _check_bandwidth_numba(box_c: np.ndarray, max_ratio: float,
                           bw_window: int) -> bool:
    """方法: Bollinger 带宽"""
    valid = ~np.isnan(box_c)
    if valid.sum() < bw_window:
        return False
    y = box_c[valid][-bw_window:]
    mean = np.mean(y)
    if mean <= 0:
        return False
    var = np.sum((y - mean) ** 2) / (len(y) - 1)
    std = np.sqrt(var)
    bandwidth = 2 * std / mean
    return bandwidth <= max_ratio


@njit(cache=True)
def _check_atr_numba(box_h: np.ndarray, box_l: np.ndarray,
                     box_c: np.ndarray, atr_window: int,
                     max_ratio: float) -> bool:
    """方法: ATR 比率"""
    n = len(box_c)
    if n < atr_window + 1:
        return False
    tr_vals = np.empty(n)
    tr_vals[:] = np.nan
    for i in range(1, n):
        hl = abs(box_h[i] - box_l[i])
        hc = abs(box_h[i] - box_c[i - 1])
        lc = abs(box_l[i] - box_c[i - 1])
        tr_vals[i] = max(hl, hc, lc)
    recent_tr = tr_vals[-atr_window:]
    valid_tr = recent_tr[~np.isnan(recent_tr)]
    if len(valid_tr) < atr_window // 2:
        return False
    atr = np.mean(valid_tr)
    mean_close = np.nanmean(box_c[-atr_window:])
    if mean_close <= 0:
        return False
    return (atr / mean_close) <= max_ratio


@njit(cache=True)
def _combine_box_checks(pass_range: bool, pass_slope: bool,
                        pass_bw: bool, pass_atr: bool,
                        method: int) -> bool:
    """根据 method 组合各检查结果。"""
    if method == _RANGE:
        return pass_range
    elif method == _SLOPE:
        return pass_slope
    elif method == _BANDWIDTH:
        return pass_bw
    elif method == _ATR:
        return pass_atr
    elif method == _ANY:
        return pass_range or pass_slope or pass_bw or pass_atr
    else:  # _ALL
        return pass_range and pass_slope and pass_bw and pass_atr


@njit(cache=True)
def box_consol_numba_v2(arr_c: np.ndarray, arr_h: np.ndarray,
                        arr_l: np.ndarray,
                        lookback_days: int,
                        recent_days: int,
                        box_method: int = _ALL,
                        max_range_ratio: float = 1.30,
                        max_slope_ratio: float = 0.001,
                        max_bandwidth_ratio: float = 0.15,
                        max_atr_ratio: float = 0.03,
                        bandwidth_window: int = 20,
                        atr_window: int = 14,
                        min_valid_points: int = 5,
                        max_daily_amp: float = 0.03,
                        max_total_range: float = 0.10) -> np.ndarray:
    """
    BoxConsolidation v2 numba 内核。

    新增: 日振幅检查 + 总振幅检查始终应用，
          同时保留 range/slope/bandwidth/atr 多种检测方法。
    """
    n = len(arr_c)
    out = np.zeros(n, dtype=np.bool_)

    for i in range(recent_days, n):
        win_start = max(0, i - lookback_days)
        if np.isnan(arr_c[i]):
            continue

        # 找窗口内最低价位置（作为箱体起点）
        min_val = np.inf
        min_pos = 0
        for j in range(win_start, i):
            v = arr_c[j]
            if v < min_val:
                min_val = v
                min_pos = j

        if np.isnan(min_val) or min_val <= 0:
            continue

        box_start = min_pos
        box_end = i - recent_days
        if box_start >= box_end:
            continue

        # 提取箱体数据
        box_c = arr_c[box_start: box_end + 1]
        box_h = arr_h[box_start: box_end + 1]
        box_l = arr_l[box_start: box_end + 1]

        # 过滤 NaN
        valid = ~(np.isnan(box_h) | np.isnan(box_l) | np.isnan(box_c))
        if valid.sum() < min_valid_points:
            continue

        # 提取有效值
        valid_idx = np.where(valid)[0]
        nv = len(valid_idx)
        box_c_v = np.empty(nv)
        box_h_v = np.empty(nv)
        box_l_v = np.empty(nv)
        for vi in range(nv):
            idx = valid_idx[vi]
            box_c_v[vi] = box_c[idx]
            box_h_v[vi] = box_h[idx]
            box_l_v[vi] = box_l[idx]

        # ── 新增: 日振幅检查（始终应用）───────────────────
        daily_ok = True
        for k in range(nv):
            amp = abs(box_h_v[k] / box_l_v[k] - 1.0)
            if amp >= max_daily_amp:
                daily_ok = False
                break

        if not daily_ok:
            continue

        # ── 新增: 总振幅检查（始终应用）───────────────────
        box_h_max = -np.inf
        box_l_min = np.inf
        for k in range(nv):
            if box_h_v[k] > box_h_max:
                box_h_max = box_h_v[k]
            if box_l_v[k] < box_l_min:
                box_l_min = box_l_v[k]
        total_range = abs(box_h_max / box_l_min - 1.0)
        if total_range >= max_total_range:
            continue

        # ── 原有: 各方法检测 ─────────────────────────────
        pass_range = _check_range_numba(box_h_v, box_l_v, max_range_ratio)
        pass_slope = _check_slope_numba(box_c_v, max_slope_ratio)
        pass_bw = _check_bandwidth_numba(box_c_v, max_bandwidth_ratio,
                                         bandwidth_window)
        pass_atr = _check_atr_numba(box_h_v, box_l_v, box_c_v,
                                    atr_window, max_atr_ratio)

        if _combine_box_checks(pass_range, pass_slope, pass_bw,
                               pass_atr, box_method):
            out[i] = True

    return out


# ====================================================================
#  3. VolumeBreakoutSignal
#  保留原版：涨幅>6%, 量>20日均量×3, 突箱体顶, 20日内首次
# ====================================================================

@njit(cache=True)
def volume_breakout_numba(arr_c: np.ndarray, arr_o: np.ndarray,
                          arr_h: np.ndarray, arr_v: np.ndarray,
                          recent_days: int, gain_threshold: float,
                          vol_multiple: float,
                          vol_ma_days: int) -> np.ndarray:
    n = len(arr_c)
    out = np.zeros(n, dtype=np.bool_)
    lookback = recent_days
    vol_ma = vol_ma_days

    for i in range(lookback + vol_ma, n):
        current_close = arr_c[i]
        current_open = arr_o[i]
        current_vol = arr_v[i]

        if np.isnan(current_close) or np.isnan(current_open):
            continue
        if current_open <= 0:
            continue

        gain = current_close / current_open - 1.0
        if gain < gain_threshold:
            continue

        vol_sum = 0.0
        vol_cnt = 0
        for j in range(i - vol_ma, i):
            if not np.isnan(arr_v[j]):
                vol_sum += arr_v[j]
                vol_cnt += 1
        avg_vol = vol_sum / vol_cnt if vol_cnt > 0 else 0.0
        if avg_vol <= 0 or current_vol < avg_vol * vol_multiple:
            continue

        high_max = -np.inf
        for j in range(i - lookback, i):
            if arr_h[j] > high_max:
                high_max = arr_h[j]
        if current_close <= high_max:
            continue

        already = False
        for j in range(i - lookback, i):
            if out[j]:
                already = True
                break
        if already:
            continue

        out[i] = True

    return out


# ====================================================================
#  4. ShortBoxConsolidationSignal (NEW)
#  条件：2-5日, 价在ref高低间, 量<ref量×50%
# ====================================================================

@njit(cache=True)
def short_box_consol_numba(arr_h: np.ndarray, arr_l: np.ndarray,
                           arr_v: np.ndarray,
                           ref_high: float, ref_low: float,
                           ref_volume: float,
                           pullback_min: int = 2,
                           pullback_max: int = 5) -> np.ndarray:
    n = len(arr_h)
    out = np.zeros(n, dtype=np.bool_)
    vol_threshold = ref_volume * 0.5

    for i in range(pullback_min - 1, n):
        start = max(0, i - pullback_max + 1)
        consol_days = i - start + 1
        if consol_days < pullback_min:
            continue

        all_ok = True
        for j in range(start, i + 1):
            if np.isnan(arr_h[j]) or np.isnan(arr_l[j]) or np.isnan(arr_v[j]):
                all_ok = False
                break
            if arr_h[j] > ref_high or arr_l[j] < ref_low or arr_v[j] >= vol_threshold:
                all_ok = False
                break

        if all_ok:
            out[i] = True

    return out


# ====================================================================
#  5. PullbackConfirmSignal (v2)
#  条件：涨幅>5%, 量>ref量×50%, close>ref_close
# ====================================================================

@njit(cache=True)
def pullback_confirm_numba_v2(arr_c: np.ndarray, arr_o: np.ndarray,
                              arr_v: np.ndarray,
                              ref_close: float, ref_volume: float,
                              confirm_gain: float = 0.05) -> np.ndarray:
    n = len(arr_c)
    out = np.zeros(n, dtype=np.bool_)
    vol_threshold = ref_volume * 0.5

    for i in range(n):
        if np.isnan(arr_c[i]) or np.isnan(arr_o[i]) or arr_o[i] <= 0:
            continue
        if np.isnan(arr_v[i]):
            continue

        gain = arr_c[i] / arr_o[i] - 1.0
        if gain < confirm_gain:
            continue
        if arr_v[i] < vol_threshold:
            continue
        if arr_c[i] <= ref_close:
            continue

        out[i] = True

    return out
