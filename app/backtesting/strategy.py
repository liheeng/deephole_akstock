from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import akshare as ak
import pandas as pd
from db.stock_daily_util import get_symbol_data, get_symbols


# ==============================
# 【基础框架】策略组合引擎（核心）
# ==============================
class CombinedStrategy(Strategy):
    """组合策略基类，支持 & | 逻辑"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.strategies = []
        self.operator = "AND"  # AND / OR

    def add_strategy(self, strat):
        self.strategies.append(strat)

    def should_buy(self):
        buy_signals = [s.should_buy() for s in self.strategies]
        if self.operator == "AND":
            return all(buy_signals)
        elif self.operator == "OR":
            return any(buy_signals)

    def should_sell(self):
        sell_signals = [s.should_sell() for s in self.strategies]
        if self.operator == "AND":
            return all(sell_signals)
        elif self.operator == "OR":
            return any(sell_signals)

    def next(self):
        if self.should_buy():
            self.buy()
        if self.should_sell():
            self.position.close()


# # 重载运算符，实现 s1 & s2 、s1 | s2
# def __and__(self, other):
#     combo = CombinedStrategy()
#     combo.add_strategy(self)
#     combo.add_strategy(other)
#     combo.operator = "AND"
#     return combo


# def __or__(self, other):
#     combo = CombinedStrategy()
#     combo.add_strategy(self)
#     combo.add_strategy(other)
#     combo.operator = "OR"
#     return combo


# ==============================
# 【可复用】单个策略模板
# ==============================
class BaseStrategy(Strategy):
    def init(self):
        pass

    def should_buy(self):
        return False
    
    def should_sell(self):
        return False
    
    def next(self):
        if self.should_buy(): self.buy()
        if self.should_sell(): self.position.close()

    def __and__(self, other):
        combo = CombinedStrategy()
        combo.add_strategy(self)
        combo.add_strategy(other)
        combo.operator = "AND"
        return combo

    def __or__(self, other):
        combo = CombinedStrategy()
        combo.add_strategy(self)
        combo.add_strategy(other)
        combo.operator = "OR"
        return combo


# ==============================
# 【策略1】均线策略
# ==============================
class MAStrategy(BaseStrategy):
    def __init__(self, period=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.period = period

    def init(self):
        self.ma = self.I(lambda x: x.rolling(self.period).mean(), self.data.close)

    def should_buy(self):
        return crossover(self.data.close, self.ma)

    def should_sell(self):
        return crossover(self.ma, self.data.close)


# ==============================
# 【策略2】涨跌停过滤策略（示例）
# ==============================
class LimitFilterStrategy(BaseStrategy):
    def should_buy(self):
        # 非涨停才允许买
        return self.data.close < self.data.high

    def should_sell(self):
        # 非跌停才允许卖
        return self.data.close > self.data.low


# ==============================
# 回测示例
# ==============================

if __name__ == "__main__":
    def get_data(code="600000"):
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        df = df[["日期","开盘","收盘","最高","最低"]]
        df.columns = ["Date","Open","Close","High","Low"]
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        return df

    # 1. 定义策略
    s5 = MAStrategy(period=5)    # 5日均线
    s10 = MAStrategy(period=10)  # 10日均线
    s_limit = LimitFilterStrategy()

    # 2. 组合策略（支持任意多层嵌套）
    # 示例：(上穿MA5 或 上穿MA10) 并且 非涨停
    strategy = (s5 | s10) & s_limit

    # 3. 回测
    df = get_symbol_data(symbol="603259.SH", start_date="2010-01-01", end_date="2026-03-31")
    bt = Backtest(df, strategy, cash=100000, commission=0.0003)
    stats = bt.run()
    print(stats)
    bt.plot()  # 本地打开图表