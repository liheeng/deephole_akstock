
from vectorbt_test.core.expr_parser import ExprParser
from vectorbt_test.core.registry import NodeRegistry


class NodeBuilder:

    def __init__(self):
        self.parser = ExprParser()

    def build(self, expr_str: str):
        return self.parser.parse(expr_str)

    def suggest(self, prefix: str) -> list:
        return [k for k in NodeRegistry._meta.keys() if k.startswith(prefix)]
