from .node import Node, NodeType


class SignalNode(Node):
    def __init__(self):
        super().__init__(NodeType.Signal)


# NodeRegistry.register("cross", lambda: CrossSignalNode(), group="signal")
# NodeRegistry.register("breakout", lambda: BreakoutSignalNode(), group="signal")