from __future__ import annotations
from typing import Dict
from vectorbt_test.core.base import GeneralExpr
from vectorbt_test.core.expr_parser import ExprParser
from vectorbt_test.core.registry import NodeRegistry
from vectorbt_test.core.nodes import NodeType
# from vectorbt_test.core.factors import Factor


class NodeBuilder:
    def __init__(self):
        self.parser = ExprParser()

    def build(self, expr_str: str):
        return self.parser.parse(expr_str)

    def build_factor(self, expr_arg: str | GeneralExpr | Dict[str, str], wrap_call):
        from vectorbt_test.core.factors import Factor
        name: str | None = None
        expr: str | None = None
        if isinstance(expr_arg, Factor):
            return expr_arg
        
        if isinstance(expr_arg, dict) and "expr" in expr_arg and "name" in expr_arg:
            name = expr_arg["name"]
            expr = expr_arg["expr"]

        elif isinstance(expr_arg, GeneralExpr):
            name = expr_arg.name
            expr = expr_arg.expr

        elif isinstance(expr_arg, str):
            expr = expr_arg
        else:
            raise ValueError(f"Invalid argument type: {type(expr_arg)} ({expr_arg})")
        
        node = self.build(expr)
        if node.type != NodeType.Factor:
            return wrap_call(name, node)
        return node
    
    def suggest(self, prefix: str) -> list:
        return [k for k in NodeRegistry._meta.keys() if k.startswith(prefix)]

