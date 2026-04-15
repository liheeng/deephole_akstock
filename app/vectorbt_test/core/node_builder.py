from __future__ import annotations
from vectorbt_test.core.expr_parser import ExprParser
from vectorbt_test.core.registry import NodeRegistry
from vectorbt_test.core.nodes import NodeType


class NodeBuilder:

    def __init__(self):
        self.parser = ExprParser()

    def build(self, expr_str: str):
        return self.parser.parse(expr_str)

    def build_factor(self, expr_str: str, wrap_call):
        node = self.build(expr_str)
        if node.type != NodeType.Factor:
            return wrap_call(node)
        return node
    
    def suggest(self, prefix: str) -> list:
        return [k for k in NodeRegistry._meta.keys() if k.startswith(prefix)]

