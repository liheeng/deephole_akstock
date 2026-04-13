import enum
from abc import ABC
from vectorbt_test.core.nodes import FeatureNode, NodeType, NodeDType, to_node
from vectorbt_test.core.node_builder import NodeBuilder
from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from vectorbt_test.core.context import PortfolioContext
import pandas as pd


class SignalScope(enum.Enum):
    Null = -1
    TS = 1
    CS = 2
    TS_CS = 4


class Signal(FeatureNode, ABC):
    @staticmethod
    def build(expr_str: str):
        node = NodeBuilder().build(expr_str)
        assert node.is_signal
        return node

    def __init__(self, *args):
        super().__init__(NodeType.Signal, NodeDType.Signal)
        self.scope = SignalScope.Null
        self.args = [to_node(a) for a in args]

    def _args(self):
        return [a.cache_key() for a in self.args]
  
    def is_scope(self, scope: int) -> bool:
        return (self.scope.value & scope) == self.scope.value
    
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
    def __init__(self, signal, signal_gate):
        super().__init__()
        self.scope = SignalScope.TS_CS
        self.signal = signal
        self.signal_gate = signal_gate

    def compute(self, data, context: PortfolioContext):
        s = self.signal.evaluate(data, context)
        g = self.signal_gate.evaluate(data, context)

        return s & g


class TSSignal(Signal):
    def __init__(self):
        super().__init__()
        self.scope = SignalScope.TS


class Cooldown(TSSignal):
    def __init__(self, signal, n):
        super().__init__()
        self.signal = signal
        self.n = n

    def compute(self, data, context: PortfolioContext):
        s = self.signal.evaluate(data, context).fillna(False)

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


class Hold(TSSignal):
    def __init__(self, signal, n):
        super().__init__()
        self.signal = signal
        self.n = n

    def compute(self, data, context: PortfolioContext):
        s = self.signal.evaluate(data, context).fillna(False)

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
        self.scope = SignalScope.TS_CS
        self.left = left
        self.right = right
        self.op = op

    def is_scope(self, scope: int) -> bool:
        return (super().is_scope(scope)
                and self.left.is_scope(scope)
                and self.right.is_scope(scope))
    
    def compute(self, data, context: PortfolioContext):
        l = self.left.evaluate(data, context)
        r = self.right.evaluate(data, context)

        if self.op == "and":
            return l & r
        elif self.op == "or":
            return l | r


class Cross(Signal):
    def __init__(self, left, right):
        super().__init__()
        self.scope = SignalScope.TS_CS
        self.left = left
        self.right = right

    def compute(self, data, context: PortfolioContext):
        a = self.left.evaluate(data, context)
        b = self.right.evaluate(data, context)

        # return (a > b) & (a.shift(1) <= b.shift(1))
        return self.apply(
            a,
            lambda x: (x > b.loc[x.index]) & (x.shift(1) <= b.loc[x.index].shift(1)),
            context
        )


class CrossUnder(Signal):
    def __init__(self, left, right):
        super().__init__()
        self.scope = SignalScope.TS_CS
        self.left = left
        self.right = right

    def compute(self, data, context: PortfolioContext):
        a = self.left.evaluate(data, context)
        b = self.right.evaluate(data, context)

        return (a < b) & (a.shift(1) >= b.shift(1))


class CSSignal(Signal):
    def __init__(self):
        super().__init__()
        self.scope = SignalScope.CS


class RebalanceDaily(CSSignal):
    def compute(self, data, context: PortfolioContext):
        return pd.Series(True, index=data.index)


class RebalanceWeekly(CSSignal):

    def __init__(self, weekday=0):  # 0=Monday
        super().__init__()
        self.weekday = weekday

    def compute(self, data, context: PortfolioContext):
        dt = data.index
        return pd.Series(dt.weekday == self.weekday, index=dt)
    

class RebalanceMonthly(CSSignal):
    def __init__(self, day=1):
        super().__init__()
        self.day = day

    def compute(self, data, context: PortfolioContext):
        dt = data.index
        return pd.Series(dt.day == self.day, index=dt)


class RebalanceEveryNDays(CSSignal):
    def __init__(self, n):
        super().__init__()
        self.n = n

    def compute(self, data, context: PortfolioContext):
        idx = data.index
        mask = pd.Series(False, index=idx)
        mask.iloc[::self.n] = True
        return mask


class RebalanceOnDates(CSSignal):
    def __init__(self, dates):
        super().__init__()
        self.dates = pd.to_datetime(dates)

    def compute(self, data, context: PortfolioContext):
        idx = data.index
        return pd.Series(idx.isin(self.dates), index=idx)


class RebalanceMonthEnd(CSSignal):
    def compute(self, data, context: PortfolioContext):
        idx = data.index
        df = pd.Series(index=idx, data=False)

        month = idx.to_period("M")
        last_days = idx.to_series().groupby(month).idxmax()

        df.loc[last_days.values] = True
        return df  


class RebalanceWeekEnd(CSSignal):
    def compute(self, data, context: PortfolioContext):
        idx = data.index
        df = pd.Series(False, index=idx)

        week = idx.to_period("W")
        last_days = idx.to_series().groupby(week).idxmax()

        df.loc[last_days.values] = True
        return df


NodeRegistry.register(
    "Cooldown",
    lambda signal, n: Cross(signal, n),
    NodeMeta(
        name="Cooldown",
        group="signal",
        desc="Cooldown Signal",
        params=[
            NodeParam("signal", "Signal", desc="Signal"),
            NodeParam("n", "int", desc="number")
        ]
    ))


NodeRegistry.register(
    "Hold",
    lambda signal, n: Hold(signal, n),
    NodeMeta(
        name="Hold",
        group="signal",
        desc="Hold Signal",
        params=[
            NodeParam("signal", "Signal", desc="Signal"),
            NodeParam("n", "int", desc="hold days")
        ]
    ))

NodeRegistry.register(
    "Cross",
    lambda left, right: Cross(left, right),
    NodeMeta(
        name="Cross",
        group="signal",
        desc="Corss Signal",
        params=[
            NodeParam("left", "Node", desc="左节点"),
            NodeParam("right", "Node", desc="右节点")
        ]
    ))

NodeRegistry.register(
    "CrossUnder",
    lambda left, right: CrossUnder(left, right),
    NodeMeta(
        name="CrossUnder",
        group="signal",
        desc="CrossUnder Signal",
        params=[
            NodeParam("left", "Node", desc="左节点"),
            NodeParam("right", "Node", desc="右节点")
        ]
    ))

NodeRegistry.register(
    "RebalanceDaily",
    lambda: RebalanceDaily(),
    NodeMeta(
        name="RebalanceDaily",
        group="signal",
        desc="RebalanceDaily Signal"
    ))

NodeRegistry.register(
    "RebalanceWeekly",
    lambda weekday=0: RebalanceWeekly(weekday),
    NodeMeta(
        name="RebalanceWeekly",
        group="signal",
        desc="RebalanceWeekly Signal",
        params=[
            NodeParam("weekday", "int", 0, "weekday")
        ]
    ))

NodeRegistry.register(
    "RebalanceMonthly",
    lambda day=1: RebalanceMonthly(day),
    NodeMeta(
        name="RebalanceMonthly",
        group="signal",
        desc="RebalanceMonthly Signal",
        params=[
            NodeParam("day", "int", 1, "day")
        ]   
    ))

NodeRegistry.register(
    "RebalanceEveryNDays",
    lambda n: RebalanceEveryNDays(n),
    NodeMeta(
        name="RebalanceEveryNDays",
        group="signal",
        desc="RebalanceEveryNDays Signal",
        params=[
            NodeParam("n", "int", 1, "n")
        ]   
    ))

NodeRegistry.register(
    "RebalanceOnDates",
    lambda dates: RebalanceOnDates(dates),
    NodeMeta(
        name="RebalanceOnDates",
        group="signal",
        desc="RebalanceOnDates Signal",
        params=[
            NodeParam("dates", "list", [], "dates")
        ]   
    ))

NodeRegistry.register(
    "RebalanceMonthEnd",
    lambda: RebalanceMonthEnd(),
    NodeMeta(
        name="RebalanceMonthEnd",
        group="signal",
        desc="RebalanceMonthEnd Signal"
    ))

NodeRegistry.register(
    "RebalanceWeekEnd",
    lambda: RebalanceWeekEnd(),
    NodeMeta(
        name="RebalanceWeekEnd",
        group="signal",
        desc="RebalanceWeekEnd Signal"
    ))

# NodeRegistry.register("cross", lambda: CrossSignalNode(), group="signal")
# NodeRegistry.register("breakout", lambda: BreakoutSignalNode(), group="signal")

