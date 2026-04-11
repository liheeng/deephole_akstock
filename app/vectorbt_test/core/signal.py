from .node import Node, NodeType


class SignalNode(Node):
    def __init__(self):
        super().__init__(NodeType.Signal)

    def cross(self, b):
        return CrossNode(self, b)
    
    def crossunder(self, b):
        return CrossUnderNode(self, b)


class CrossNode(SignalNode):
    def __init__(self, a, b):
        super().__init__()
        self.a = a
        self.b = b

    def compute(self, data, cache, context):
        a = self.a.compute(data, cache, context)
        b = self.b.compute(data, cache, context)

        return (a > b) & (a.shift(1) <= b.shift(1))


class CrossUnderNode(SignalNode):
    def __init__(self, a, b):
        super().__init__()
        self.a = a
        self.b = b

    def compute(self, data, cache, context):
        a = self.a.compute(data, cache, context)
        b = self.b.compute(data, cache, context)

        return (a < b) & (a.shift(1) >= b.shift(1))
    
# NodeRegistry.register("cross", lambda: CrossSignalNode(), group="signal")
# NodeRegistry.register("breakout", lambda: BreakoutSignalNode(), group="signal")