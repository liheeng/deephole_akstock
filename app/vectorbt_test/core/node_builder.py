
from .expr_parser import ExprParser
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
                return NodeRegistry.create(name, **params)

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
    
    def suggest(self, prefix: str) -> list:
        return [k for k in NodeRegistry._meta.keys() if k.startswith(prefix)]
