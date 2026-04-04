import enum
from abc import ABC, abstractmethod
from typing import List
import pandas as pd
from vectorbt_test.signals.base_signal import BaseSignal


class SignalValue(enum.Enum):
    BUY = 1
    SELL = -1
    NONE = 0


class BaseExpr(ABC):
    @abstractmethod
    def signals(self) -> List[BaseSignal]:
        pass

    @abstractmethod
    def evaluate(self, signal_values: dict) -> pd.Series:
        pass

    def __and__(self, other: "BaseExpr") -> "BaseExpr":
        return AndExpr(self, other)

    def And(self, other: "BaseExpr") -> "BaseExpr":
        return AndExpr(self, other)
    
    def __or__(self, other: "BaseExpr") -> "BaseExpr":
        return OrExpr(self, other)

    def Or(self, other: "BaseExpr") -> "BaseExpr":
        return OrExpr(self, other)
    
    def __not__(self) -> "BaseExpr":
        return NotExpr(self)
    
    def Not(self) -> "BaseExpr":
        return NotExpr(self)
    
    def __invert__(self):
        return NotExpr(self)


# 叶子节点：单个信号的表达式
class SignalExpr(BaseExpr):
    def __init__(self, signal: BaseSignal, signal_value: SignalValue = SignalValue.NONE):
        self.signal = signal
        self.signal_value = signal_value

    def signals(self) -> List[BaseSignal]:
        return [self.signal]
    
    def evaluate(self, signal_values: dict) -> pd.Series:
        series = signal_values[self.signal.name]
        return series == self.signal_value.value


class AndExpr(BaseExpr):
    def __init__(self, left: "BaseExpr", right: "BaseExpr"):
        self.left = left
        self.right = right

    def signals(self) -> List[BaseSignal]:
        return self.left.signals() + self.right.signals()
    
    def evaluate(self, signal_values) -> pd.Series:
        return self.left.evaluate(signal_values) & self.right.evaluate(signal_values)


class OrExpr(BaseExpr):
    def __init__(self, left: "BaseExpr", right: "BaseExpr"):
        self.left = left
        self.right = right

    def signals(self) -> List[BaseSignal]:
        return self.left.signals() + self.right.signals()
    
    def evaluate(self, signal_values: dict) -> pd.Series:
        return self.left.evaluate(signal_values) | self.right.evaluate(signal_values)


class NotExpr(BaseExpr):
    def __init__(self, expr: "BaseExpr"):
        self.expr = expr

    def signals(self) -> List[BaseSignal]:
        return self.expr.signals()
    
    def evaluate(self, signal_values: dict) -> pd.Series:
        return ~self.expr.evaluate(signal_values)


def buy_signal_expr(signal: BaseSignal) -> BaseExpr:
    return SignalExpr(signal, signal_value=SignalValue.BUY)


def sell_signal_expr(signal: BaseSignal) -> BaseExpr:
    return SignalExpr(signal, signal_value=SignalValue.SELL)


class Trigger:
    def __init__(self, expr: BaseExpr):
        self.expr = expr

    def signals(self):
        return self.expr.signals()
    
    def check(self, signal_values: dict) -> pd.Series:
        # Update signal values before evaluation
        return self.expr.evaluate(signal_values)
