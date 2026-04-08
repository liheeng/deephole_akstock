import enum
from abc import ABC, abstractmethod
from typing import List
import pandas as pd
from vectorbt_test.core.base_signal import BaseSignal


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

    # ===== 布尔运算 =====
    def __and__(self, other: "BaseExpr") -> "BaseExpr":
        return AndExpr(self, other)

    def __or__(self, other: "BaseExpr") -> "BaseExpr":
        return OrExpr(self, other)

    def __invert__(self):
        return NotExpr(self)

    def _to_expr(self, other):
        if isinstance(other, BaseExpr):
            return other
        return ConstExpr(other)

    # ===== 数值运算（新增）=====
    def __add__(self, other: "BaseExpr") -> "BaseExpr":
        return AddExpr(self, self._to_expr(other))

    def __sub__(self, other: "BaseExpr") -> "BaseExpr":
        return SubExpr(self, self._to_expr(other))

    def __mul__(self, other: "BaseExpr") -> "BaseExpr":
        return MulExpr(self, self._to_expr(other))

    def __truediv__(self, other: "BaseExpr") -> "BaseExpr":
        return DivExpr(self, self._to_expr(other))


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


class ScoreSignalExpr(BaseExpr):
    def __init__(self, signal: BaseSignal, weight: float = 1.0):
        self.signal = signal
        self.weight = weight

    def signals(self) -> List[BaseSignal]:
        return [self.signal]
    
    def evaluate(self, signal_values: dict) -> pd.Series:
        return signal_values[self.signal.name] * self.weight


class BinaryScoreExpr(BaseExpr):
    def __init__(self, left: BaseExpr, right: BaseExpr):
        self.left = left
        self.right = right

    def signals(self):
        return list(set(self.left.signals() + self.right.signals()))


class AddExpr(BinaryScoreExpr):
    def evaluate(self, signal_values):
        return self.left.evaluate(signal_values) + self.right.evaluate(signal_values)


class SubExpr(BinaryScoreExpr):
    def evaluate(self, signal_values):
        return self.left.evaluate(signal_values) - self.right.evaluate(signal_values)


class MulExpr(BinaryScoreExpr):
    def evaluate(self, signal_values):
        return self.left.evaluate(signal_values) * self.right.evaluate(signal_values)


class DivExpr(BinaryScoreExpr):
    def evaluate(self, signal_values):
        right = self.right.evaluate(signal_values)
        return self.left.evaluate(signal_values) / right.replace(0, 1e-9)


class ConstExpr(BaseExpr):
    def __init__(self, value: float):
        self.value = value

    def signals(self):
        return []

    def evaluate(self, signal_values):
        # 用任意一个 index
        any_series = next(iter(signal_values.values()))
        return pd.Series(self.value, index=any_series.index)


def buy_signal_expr(signal: BaseSignal) -> BaseExpr:
    return SignalExpr(signal, signal_value=SignalValue.BUY)


def sell_signal_expr(signal: BaseSignal) -> BaseExpr:
    return SignalExpr(signal, signal_value=SignalValue.SELL)


def score_signal_expr(signal: BaseSignal, weight: float = 1.0) -> BaseExpr:
    return ScoreSignalExpr(signal, weight=weight)