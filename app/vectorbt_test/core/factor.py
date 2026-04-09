from .signal_expr import BaseExpr
import pandas as pd


class Factor:
    def __init__(self, name: str, expr: BaseExpr):
        self.name = name
        self.expr = expr
        self._signals = self._collect_signals()

    def _collect_signals(self):
        # 去重（关键）
        signals = self.expr.signals()
        return {s.name: s for s in signals}

    def signals(self):
        return list(self._signals.values())

    # ==============================
    # 核心执行入口（你现在缺的）
    # ==============================
    def run(self, signal_values: dict) -> pd.Series:
        return self.expr.evaluate(signal_values)

    # ==============================
    # 因子分数（带标准化）
    # ==============================
    def score(self, signal_values: dict) -> pd.Series:
        s = self.run(signal_values)
        # 标准化（时间序列 or 横截面你后面可以扩展）
        return (s - s.mean()) / (s.std() + 1e-9)

    # ==============================
    # 排名（选股核心）
    # ==============================
    def rank(self, signal_values: dict) -> pd.Series:
        s = self.score(signal_values)
        return s.rank(ascending=False)

    # ==============================
    # TopN（直接生成信号）
    # ==============================
    def top_n(self, signal_values, n: int) -> pd.Series:
        rank = self.rank(signal_values)
        return rank <= n
    
    def check(self, signal_values: dict) -> pd.Series:
        # Update signal values before evaluation
        return self.expr.evaluate(signal_values)