from __future__ import annotations
from vectorbt_test.core.base import GeneralExpr
from vectorbt_test.core.expr_parser import ExprParser
from vectorbt_test.core.registry import NodeRegistry
from vectorbt_test.core.nodes import NodeType


class NodeBuilder:

    def __init__(self):
        self.parser = ExprParser()

    def build(self, expr_str: str):
        return self.parser.parse(expr_str)

    def build_factor(self, expr_str: str | GeneralExpr, wrap_call):
        name: str | None = None
        if isinstance(expr_str, GeneralExpr):
            name = expr_str.name
            expr_str = expr_str.expr

        node = self.build(expr_str)
        if node.type != NodeType.Factor:
            return wrap_call(name, node)
        return node
    
    def suggest(self, prefix: str) -> list:
        return [k for k in NodeRegistry._meta.keys() if k.startswith(prefix)]

