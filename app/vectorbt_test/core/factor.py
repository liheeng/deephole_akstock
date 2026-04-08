from .signal_expr import BaseExpr
import pandas as pd


class Factor:
    def __init__(self, name: str, expr: BaseExpr):
        self.name = name
        self.expr = expr

    def signals(self):
        return self.expr.signals()
    
    def check(self, signal_values: dict) -> pd.Series:
        # Update signal values before evaluation
        return self.expr.evaluate(signal_values)
    
    def score(self, signal_values):
        return self.expr.evaluate(signal_values)