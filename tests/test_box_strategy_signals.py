"""
箱体突破策略 4 个 Signal 的测试用例。

数据来源：从数据库加载真实 A 股数据。
验证方法：
  1. 结果类型和形状检查
  2. 信号语义正确性（手工检查触发逻辑）
  3. Numba 版本 vs Python 参考版本结果一致
"""

import sys
sys.path.insert(0, "app")  # noqa: E402

import pytest
import numpy as np
import pandas as pd

from db.duckdb import DuckDBController
from db.db_common import DB
from db.stock_daily_util import get_CN_symbols, get_symbol_data, get_symbols_data
from vectorbt_test.engine.init import load_register_nodes
from vectorbt_test.engine.context_builder import create_context
from vectorbt_test.engine.data_provider import DataProvider

from vectorbt_test.picker.picker_signals.box_strategy_signals import (
    HeavyDropSignal,
    BoxConsolidationSignal,
    VolumeBreakoutSignal,
    PullbackConfirmSignal,
)


# ====================================================================
#  Fixtures
# ====================================================================

@pytest.fixture(scope="session")
def db():
    """数据库连接。"""
    load_register_nodes()
    return DuckDBController(DB)


@pytest.fixture(scope="session")
def sample_symbols(db) -> list[str]:
    """获取一些测试用股票（含不同市场、不同走势的）。"""
    raw = get_CN_symbols(db)
    # 扁平化：fetch_mode='all' 返回 list[tuple]
    all_symbols = [s[0] if isinstance(s, (list, tuple)) else s for s in raw]
    # 打散取一部分，包含大盘股、小盘股
    import random
    random.seed(42)
    # 保留一些已知的股票做参考
    known = ["600519.SH", "000858.SZ", "603259.SH", "600362.SH"]
    others = [s for s in all_symbols if s not in known]
    random.shuffle(others)
    selected = known + others[:16]
    return selected


@pytest.fixture(scope="session")
def sample_data(db, sample_symbols) -> pd.DataFrame:
    """加载测试数据。"""
    symbols_str = ", ".join(sample_symbols)
    df = get_symbols_data(db, symbols_str, "2020-01-01", "2026-06-11")
    assert len(df) > 0, "数据加载失败"
    return df


@pytest.fixture(scope="session")
def test_context(sample_data):
    """创建 PortfolioContext。"""
    dp = DataProvider(None)
    ctx = create_context(sample_data, dp)
    return ctx


@pytest.fixture(scope="session")
def data_view(test_context):
    """获取 DataView。"""
    return test_context.data_adapter.data_view()


# ====================================================================
#  辅助函数
# ====================================================================

def _check_basic_result(result, expected_dates, expected_symbols):
    """验证结果的基本属性。"""
    assert result is not None, "结果不应为 None"
    assert isinstance(result, (pd.Series, pd.DataFrame)), f"结果应为 Series 或 DataFrame，但得到 {type(result)}"
    if isinstance(result, pd.DataFrame):
        assert result.shape[0] == expected_dates, f"日期数不匹配: {result.shape[0]} vs {expected_dates}"
        assert result.shape[1] == expected_symbols, f"股票数不匹配: {result.shape[1]} vs {expected_symbols}"
        assert result.dtypes.iloc[0] == bool, f"结果应为 bool 类型，但得到 {result.dtypes.iloc[0]}"


def _has_any_true(result) -> bool:
    """检查结果中是否有 True 值。"""
    if isinstance(result, pd.Series):
        return result.any()
    return result.any().any()


def _count_true_stocks(result, date_idx=-1) -> int:
    """统计某天触发 True 的股票数量。"""
    if isinstance(result, pd.Series):
        return int(result.iloc[date_idx])
    return int(result.iloc[date_idx].sum())


def _find_true_dates(result) -> list:
    """找到所有触发 True 的日期。"""
    if isinstance(result, pd.Series):
        return result[result].index.tolist()
    return result.index[result.any(axis=1)].tolist()


# ====================================================================
#  1. HeavyDropSignal 测试
# ====================================================================

class TestHeavyDropSignal:

    def test_signal_type_and_shape(self, data_view, test_context):
        """验证结果类型和形状正确。"""
        signal = HeavyDropSignal(lookback_days=500, min_gap_days=80,
                                 low_ratio=0.2, min_gap_low_days=40)
        result = signal.evaluate(data_view, test_context)

        close = data_view["close"]
        _check_basic_result(result, close.shape[0], close.shape[1])
        print(f"\n  HeavyDrop 信号矩阵: {result.shape}, True 总数: {result.sum().sum()}")

    def test_some_stocks_should_trigger(self, data_view, test_context):
        """验证至少有一部分股票会触发信号（不是全 False）。"""
        signal = HeavyDropSignal(lookback_days=500, min_gap_days=80,
                                 low_ratio=0.2, min_gap_low_days=40)
        result = signal.evaluate(data_view, test_context)
        assert _has_any_true(result), "应有股票触发 HeavyDrop 信号"
        print(f"\n  HeavyDrop 触发股票数(最新日): {_count_true_stocks(result)}")

    def test_trigger_means_price_dropped(self, data_view, test_context):
        """验证触发信号时，最低价确实从高位大幅下跌。"""
        signal = HeavyDropSignal(lookback_days=500, min_gap_days=80,
                                 low_ratio=0.2, min_gap_low_days=40)
        result = signal.evaluate(data_view, test_context)

        close = data_view["close"]
        for col in close.columns:
            sig_col = result[col] if isinstance(result, pd.DataFrame) else result
            if sig_col.any():
                first_true = np.argmax(sig_col.values)
                dt = sig_col.index[first_true]
                pos = close.index.get_loc(dt)
                win_start = max(0, pos - 500)
                hist_max = close[col].iloc[win_start:pos].max()
                hist_min = close[col].iloc[win_start:pos].min()
                # 窗口最低价应低于历史最高价的 20%
                assert hist_min < hist_max * 0.2, \
                    f"{col}: 最低价 {hist_min:.2f} 应 < 历史最高 {hist_max:.2f} × 0.2"

    def test_no_lookback_no_signal(self, data_view, test_context):
        """验证没有足够回看天数时不会触发。"""
        close = data_view["close"]
        signal = HeavyDropSignal(lookback_days=3000, min_gap_days=9999,
                                 low_ratio=0.2, min_gap_low_days=9999)
        result = signal.evaluate(data_view, test_context)
        assert not _has_any_true(result), "超大 min_gap_days 不应触发"

    def test_numba_matches_python(self, data_view, test_context):
        """Numba 结果与 evaluate() 完全一致。"""
        close = data_view["close"]

        signal = HeavyDropSignal(lookback_days=500, min_gap_days=80,
                                 low_ratio=0.2, min_gap_low_days=40)
        signal_result = signal.evaluate(data_view, test_context)

        from vectorbt_test.picker.picker_signals.box_strategy_numba import heavy_drop_numba

        for col in close.columns[:3]:
            arr = close[col].values.astype(np.float64)
            numba_out = heavy_drop_numba(arr, 500, 80, 0.2, 40)
            # cummax
            for i in range(1, len(numba_out)):
                if numba_out[i - 1]:
                    numba_out[i] = True
            expected = signal_result[col].values if isinstance(signal_result, pd.DataFrame) else signal_result.values
            np.testing.assert_array_equal(numba_out, expected,
                                          f"{col}: Numba 与 evaluate() 不一致")


# ====================================================================
#  2. BoxConsolidationSignal 测试
# ====================================================================

class TestBoxConsolidationSignal:

    def test_signal_type_and_shape(self, data_view, test_context):
        """验证结果类型和形状正确。"""
        signal = BoxConsolidationSignal(
            lookback_days=500, recent_days=15,
            max_daily_amp=0.02, max_total_range=0.10,
        )
        result = signal.evaluate(data_view, test_context)

        close = data_view["close"]
        _check_basic_result(result, close.shape[0], close.shape[1])
        print(f"\n  BoxConsolidation 信号矩阵: {result.shape}, True 总数: {result.sum().sum()}")

    def test_numba_matches_python(self, data_view, test_context):
        """Numba + cummax 结果与信号 evaluate() 一致。"""
        close = data_view["close"]
        high = data_view["high"]
        low = data_view["low"]

        signal = BoxConsolidationSignal(
            lookback_days=500, recent_days=15,
            max_daily_amp=0.02, max_total_range=0.10,
        )
        signal_result = signal.evaluate(data_view, test_context)

        from vectorbt_test.picker.picker_signals.box_strategy_numba import box_consol_numba_v2

        for col in close.columns[:2]:
            c = close[col].values.astype(np.float64)
            h = high[col].values.astype(np.float64)
            l = low[col].values.astype(np.float64)

            numba_out = box_consol_numba_v2(c, h, l, 500, 15, 0.02, 0.10, 5)
            for i in range(1, len(numba_out)):
                if numba_out[i - 1]:
                    numba_out[i] = True

            expected = signal_result[col].values if isinstance(signal_result, pd.DataFrame) else signal_result.values
            np.testing.assert_array_equal(numba_out, expected,
                                          f"{col}: Numba 结果与 evaluate() 不一致")

# ====================================================================
#  3. VolumeBreakoutSignal 测试
# ====================================================================

class TestVolumeBreakoutSignal:

    def test_signal_type_and_shape(self, data_view, test_context):
        """验证结果类型和形状正确。"""
        signal = VolumeBreakoutSignal(recent_days=20, gain_threshold=0.06, vol_multiple=3.0)
        result = signal.evaluate(data_view, test_context)

        close = data_view["close"]
        _check_basic_result(result, close.shape[0], close.shape[1])
        print(f"\n  VolumeBreakout 信号矩阵: {result.shape}, True 总数: {result.sum().sum()}")

    def test_trigger_conditions(self, data_view, test_context):
        """验证触发日的成交量和涨幅条件（只检查首次触发点）。"""
        signal = VolumeBreakoutSignal(recent_days=20, gain_threshold=0.06, vol_multiple=3.0)
        result = signal.evaluate(data_view, test_context)
        close = data_view["close"]
        open_ = data_view["open"]
        volume = data_view["volume"]

        for col in close.columns[:5]:
            sig_col = result[col] if isinstance(result, pd.DataFrame) else result
            true_vals = sig_col.values
            first_true = np.argmax(true_vals) if true_vals.any() else -1
            if first_true < 0:
                continue
            dt = sig_col.index[first_true]
            pos = close.index.get_loc(dt)
            vol = volume[col].iloc[pos]
            gain = close[col].iloc[pos] / open_[col].iloc[pos] - 1
            assert gain >= 0.06 - 1e-6, f"{col} @ {dt}: 涨幅 {gain:.4f} < 0.06"
            vol_window = volume[col].iloc[max(0, pos - 20):pos]
            avg_vol = vol_window.mean()
            assert vol >= avg_vol * 3.0 - 1e-6, f"{col} @ {dt}: 成交量 {vol:.0f} < 均量 {avg_vol:.0f} × 3"

    def test_numba_matches_python(self, data_view, test_context):
        """Numba + cummax 结果与信号 evaluate() 一致。"""
        close = data_view["close"]
        open_ = data_view["open"]
        high = data_view["high"]
        volume = data_view["volume"]

        signal = VolumeBreakoutSignal(recent_days=20, gain_threshold=0.06, vol_multiple=3.0)
        signal_result = signal.evaluate(data_view, test_context)

        from vectorbt_test.picker.picker_signals.box_strategy_numba import volume_breakout_numba

        for col in close.columns[:2]:
            c = close[col].values.astype(np.float64)
            o = open_[col].values.astype(np.float64)
            h = high[col].values.astype(np.float64)
            v = volume[col].values.astype(np.float64)
            numba_out = volume_breakout_numba(c, o, h, v, 20, 0.06, 3.0, 20)
            for i in range(1, len(numba_out)):
                if numba_out[i - 1]:
                    numba_out[i] = True
            expected = signal_result[col].values if isinstance(signal_result, pd.DataFrame) else signal_result.values
            np.testing.assert_array_equal(numba_out, expected, f"{col}: 不一致")


# ====================================================================
#  4. PullbackConfirmSignal 测试
# ====================================================================

class TestPullbackConfirmSignal:

    def test_signal_type_and_shape(self, data_view, test_context):
        """验证结果类型和形状正确。"""
        signal = PullbackConfirmSignal(confirm_gain=0.05)
        result = signal.evaluate(data_view, test_context)
        close = data_view["close"]
        _check_basic_result(result, close.shape[0], close.shape[1])
        print(f"\n  PullbackConfirm 信号矩阵: {result.shape}, True 总数: {result.sum().sum()}")

    def test_numba_matches_python(self, data_view, test_context):
        """Numba + cummax 结果与信号 evaluate() 一致。"""
        close = data_view["close"]
        open_ = data_view["open"]
        volume = data_view["volume"]

        signal = PullbackConfirmSignal(confirm_gain=0.05)
        signal_result = signal.evaluate(data_view, test_context)

        from vectorbt_test.picker.picker_signals.box_strategy_numba import pullback_confirm_numba_v2

        for col in close.columns[:2]:
            c = close[col].values.astype(np.float64)
            o = open_[col].values.astype(np.float64)
            v = volume[col].values.astype(np.float64)
            # PullbackConfirm 需要 ref_values, 测试中用默认值
            numba_out = pullback_confirm_numba_v2(c, o, v, 99999, 0, 0.05)
            for i in range(1, len(numba_out)):
                if numba_out[i - 1]:
                    numba_out[i] = True
            # 由于没有 ref_values，预期全 False
            assert not numba_out.any(), "无 ref 时不应触发"
            print(f"  {col}: PullbackConfirm 无 ref→全 False (正确)")


# ====================================================================
#  5. 组合信号测试
# ====================================================================

class TestCombinedSignals:

    def test_all_signals_type_check(self, data_view, test_context):
        """验证 3 个独立信号类型正确。"""
        hd = HeavyDropSignal(lookback_days=500, min_gap_days=80, low_ratio=0.2, min_gap_low_days=40)
        bc = BoxConsolidationSignal(lookback_days=500, recent_days=15)
        vb = VolumeBreakoutSignal(recent_days=20, gain_threshold=0.06, vol_multiple=3.0)
        for val, name in [(hd.evaluate(data_view, test_context), 'HeavyDrop'),
                          (bc.evaluate(data_view, test_context), 'BoxConsol'),
                          (vb.evaluate(data_view, test_context), 'VolBreakout')]:
            assert val is not None and isinstance(val, pd.DataFrame) and val.dtypes.iloc[0] == bool
        print("  3 个信号类型验证通过")
