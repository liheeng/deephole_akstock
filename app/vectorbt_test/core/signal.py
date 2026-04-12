from abc import ABC
from .node import Node, NodeType
import pandas as pd


class Signal(Node, ABC):
    def __init__(self):
        super().__init__(NodeType.Signal)

    def __and__(self, other):
        return BinarySignalOp(self, other, op="and")

    def __or__(self, other):
        return BinarySignalOp(self, other, op="or")

    def when(self, schedule):
        return SignalGate(self, schedule)

    def filter(self, condition):
        return SignalGate(self, condition)

    def confirm(self, condition):
        return SignalGate(self, condition)

    def gate(self, other):
        return SignalGate(self, other)

    def cross(self, b):
        return Cross(self, b)
    
    def crossunder(self, b):
        return CrossUnder(self, b)
    
    def cooldown(self, n):
        return Cooldown(self, n)

    def hold(self, n):
        return Hold(self, n)


class SignalGate(Signal):
    def __init__(self, signal, gate):
        self.signal = signal
        self.gate = gate

    def compute(self, data, cache, context):
        s = self.signal.compute(data, cache, context)
        g = self.gate.compute(data, cache, context)

        return s & g


class Cooldown(Signal):
    def __init__(self, signal, n):
        self.signal = signal
        self.n = n

    def compute(self, data, cache, context):
        s = self.signal.compute(data, cache, context).fillna(False)

        result = s.copy()
        cooldown = 0

        for i in range(len(s)):
            if cooldown > 0:
                result.iloc[i] = False
                cooldown -= 1
            elif s.iloc[i]:
                cooldown = self.n - 1  # 当前算1天
                result.iloc[i] = True

        return result


class Hold(Signal):
    def __init__(self, signal, n):
        self.signal = signal
        self.n = n

    def compute(self, data, cache, context):
        s = self.signal.compute(data, cache, context).fillna(False)

        result = pd.Series(False, index=s.index)

        hold = 0

        for i in range(len(s)):
            if s.iloc[i]:
                hold = self.n
            if hold > 0:
                result.iloc[i] = True
                hold -= 1

        return result
    

class BinarySignalOp(Signal):
    def __init__(self, left, right, op):
        super().__init__()
        self.left = left
        self.right = right
        self.op = op

    def compute(self, data, cache, context):
        l = self.left.compute(data, cache, context)
        r = self.right.compute(data, cache, context)

        if self.op == "and":
            return l & r
        elif self.op == "or":
            return l | r


class Cross(Signal):
    def __init__(self, a, b):
        super().__init__()
        self.a = a
        self.b = b

    def compute(self, data, cache, context):
        a = self.a.compute(data, cache, context)
        b = self.b.compute(data, cache, context)

        return (a > b) & (a.shift(1) <= b.shift(1))


class CrossUnder(Signal):
    def __init__(self, a, b):
        super().__init__()
        self.a = a
        self.b = b

    def compute(self, data, cache, context):
        a = self.a.compute(data, cache, context)
        b = self.b.compute(data, cache, context)

        return (a < b) & (a.shift(1) >= b.shift(1))


class RebalanceDaily(Signal):
    def compute(self, data, cache, context):
        return pd.Series(True, index=data.index)


class RebalanceWeekly(Signal):
    def __init__(self, weekday=0):  # 0=Monday
        self.weekday = weekday

    def compute(self, data, cache, context):
        dt = data.index
        return pd.Series(dt.weekday == self.weekday, index=dt)
    

class RebalanceMonthly(Signal):
    def __init__(self, day=1):
        self.day = day

    def compute(self, data, cache, context):
        dt = data.index
        return pd.Series(dt.day == self.day, index=dt)


class RebalanceEveryNDays(Signal):
    def __init__(self, n):
        self.n = n

    def compute(self, data, cache, context):
        idx = data.index
        mask = pd.Series(False, index=idx)
        mask.iloc[::self.n] = True
        return mask


class RebalanceOnDates(Signal):
    def __init__(self, dates):
        self.dates = pd.to_datetime(dates)

    def compute(self, data, cache, context):
        idx = data.index
        return pd.Series(idx.isin(self.dates), index=idx)


class RebalanceMonthEnd(Signal):
    def compute(self, data, cache, context):
        idx = data.index
        df = pd.Series(index=idx, data=False)

        month = idx.to_period("M")
        last_days = idx.to_series().groupby(month).idxmax()

        df.loc[last_days.values] = True
        return df  


class RebalanceWeekEnd(Signal):
    def compute(self, data, cache, context):
        idx = data.index
        df = pd.Series(False, index=idx)

        week = idx.to_period("W")
        last_days = idx.to_series().groupby(week).idxmax()

        df.loc[last_days.values] = True
        return df
# NodeRegistry.register("cross", lambda: CrossSignalNode(), group="signal")
# NodeRegistry.register("breakout", lambda: BreakoutSignalNode(), group="signal")