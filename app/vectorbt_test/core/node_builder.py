
from .expr_parser import ExprParser
from .expr import NodeExpr
from .registry import NodeRegistry
import re


class NodeBuilder:

    def __init__(self):
        self.parser = ExprParser()

    def build(self, expr_str: str):

        expr_str = expr_str.strip()

        # ===== 匹配函数式 Node =====
        match = re.match(r"(\w+)\((.*?)\)", expr_str)

        if match:
            name, args = match.groups()

            if name in NodeRegistry._factories:
                params = self._parse_args(args)
                node = NodeRegistry.create(name, **params)
                return NodeExpr(node)

        # ===== 普通表达式 =====
        return self.parser.parse(expr_str)

    def _parse_args(self, args_str):
        if not args_str:
            return {}

        parts = args_str.split(",")

        params = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=")
                params[k.strip()] = eval(v.strip())
            else:
                params["period"] = int(p.strip())

        return params
    
    # def suggest(prefix):
    #     return [k for k in NodeRegistry.registry if k.startswith(prefix)]
