import enum
from dataclasses import dataclass
from typing import Sequence, Callable
from abc import ABC, abstractmethod
import pandas as pd
from vectorbt_test.core.strategy import Strategy
from vectorbt_test.core.signals import Signal, SignalGroup
from vectorbt_test.core.node_builder import NodeBuilder
import vectorbt as vbt
import numpy as np
import json
from utils.log_manager import get_logger

logger = get_logger(__name__)


class PortfolioType(enum.Enum):
    SIGNAL_STRATEGY = "signal_strategy"
    WEIGHT_STRATEGY = "weight_strategy"


@dataclass
class PortfolioParameters:
    freq: str
    init_cash: float
    top_n: int | None = 10
    hold_days: int = 5


class StrategyPortfolio(ABC):
    def __init__(self,
                 strategies: Sequence[Strategy],
                 schedule_signal: str | Signal | None = None,
                 portfolio_params: PortfolioParameters | None = None):
        self.strategies = strategies
        built_signal = NodeBuilder().build(schedule_signal) if isinstance(schedule_signal, str) else schedule_signal
        self.schedule_signal: Signal | None = built_signal if isinstance(built_signal, Signal) or built_signal is None else None
        if self.schedule_signal is not None:
            assert self.schedule_signal.is_signal and self.schedule_signal.is_group(SignalGroup.CS.value | SignalGroup.TS_CS.value)
        self.params = portfolio_params

    @abstractmethod
    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        pass


class PortfolioResultWrapper():
    def __init__(self, portfolio):
        self.portfolio = portfolio

    def _get_consistent_stats_df(self):
        """
        内部工具方法：获取一个统一结构的 DataFrame。
        确保结果：Columns 是指标名，Index 是策略/资产名。
        """
        stats_df = self.portfolio.stats(agg_func=None)
        
        # 1. 兼容单列：如果是 Series，转为 DataFrame
        if isinstance(stats_df, pd.Series):
            stats_df = stats_df.to_frame(name=self.portfolio.wrapper.columns[0])
        
        # 2. 自动转向：
        # 如果 'Sharpe Ratio' 在 index 中，说明目前是 指标x策略，需要转置成 策略x指标
        # 这样 details 转换成 dict(orient='index') 时，key 才是策略名
        if 'Sharpe Ratio' in stats_df.index:
            stats_df = stats_df.T
            
        return stats_df

    def get_pf_stats(self, agg_func: Callable = np.mean, as_json=False):
        pf = self.portfolio
        
        # 使用统一工具获取 stats_df (策略为行，指标为列)
        detailed_stats = self._get_consistent_stats_df()
        
        # 获取平均值
        avg_stats = pf.stats(agg_func=agg_func)

        # 提取冠军列名 (现在指标已经在 columns 里了)
        try:
            best_sharpe_col = detailed_stats['Sharpe Ratio'].idxmax()
            best_return_col = detailed_stats.get('Total Return [%]', detailed_stats.get('Total Return')).idxmax()
        except Exception:
            best_sharpe_col = "N/A"
            best_return_col = "N/A"

        avg_dict = avg_stats.replace([np.inf, -np.inf], np.nan).to_dict()
        avg_dict['Best Sharpe Column'] = best_sharpe_col
        avg_dict['Best Return Column'] = best_return_col

        # 生成结果
        res = {
            "average": avg_dict,
            # 因为 detailed_stats 的 index 是策略名，所以 orient='index' 会得到正确结构
            "details": detailed_stats.replace([np.inf, -np.inf], np.nan).to_dict(orient='index')
        }

        return json.dumps(res, default=str) if as_json else res

    def get_pf_value_dict(self, as_json=False):
        pf = self.portfolio
        all_values = pf.value()
        if isinstance(all_values, pd.Series):
            all_values = all_values.to_frame(name=pf.wrapper.columns[0])

        time_index = all_values.index.strftime('%Y-%m-%d %H:%M:%S').tolist()
        
        # 使用统一工具获取详细统计
        detailed_stats = self._get_consistent_stats_df()

        # 找冠军
        try:
            best_sharpe_col = detailed_stats['Sharpe Ratio'].idxmax()
            best_return_col = detailed_stats.get('Total Return [%]', detailed_stats.get('Total Return')).idxmax()
        except Exception as e:
            logger.error(f"get_pf_value_dict error: {e}")
            best_sharpe_col, best_return_col = "N/A", "N/A"

        clean_df = all_values.replace([np.inf, -np.inf], np.nan)
        
        res = {
            "times": time_index,
            "average": clean_df.mean(axis=1).tolist(),
            "details": clean_df.to_dict(orient='list'),
            "best_sharpe": clean_df[best_sharpe_col].tolist() if best_sharpe_col in clean_df.columns else [],
            "best_return": clean_df[best_return_col].tolist() if best_return_col in clean_df.columns else [],
            "meta": {
                "best_sharpe_column": str(best_sharpe_col),
                "best_return_column": str(best_return_col),
                "freq": str(pf.wrapper.freq),
                "count": len(time_index)
            }
        }

        return json.dumps(res, default=str) if as_json else res

    def trades(self):
        return self.portfolio.trades
    
    def values(self):
        return self.portfolio.value()
    
    def clean_for_json(self, obj):
        if isinstance(obj, pd.DataFrame):
            return obj.replace([np.nan, np.inf, -np.inf], None).to_dict(orient="records")

        if isinstance(obj, pd.Series):
            return obj.replace([np.nan, np.inf, -np.inf], None).to_dict()

        if isinstance(obj, dict):
            return {k: self.clean_for_json(v) for k, v in obj.items()}

        if isinstance(obj, list):
            return [self.clean_for_json(v) for v in obj]

        if isinstance(obj, float):
            if np.isnan(obj) or np.isinf(obj):
                return None

        return obj
