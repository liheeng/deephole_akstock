import ast

from vectorbt_test.core.signal_expr import BaseExpr
from vectorbt_test.core.base_signal import BaseSignal
from vectorbt_test.core.signal_expr import score_signal_expr


# !!! Need to register firstly
# SignalRegistry.register("ma5", MASignal(5))
# SignalRegistry.register("macd", MACDSignal())
# SignalRegistry.register("rsi", RSISignal(14))

class ExprParser:

    def parse(self, expr_str: str):
        tree = ast.parse(expr_str, mode="eval")
        return self._parse_node(tree.body)

    def _parse_node(self, node):

        # ===== 数字 =====
        if isinstance(node, ast.Constant):
            return node.value

        # ===== signal 名字 =====
        if isinstance(node, ast.Name):
            signal = SignalRegistry.create(node.id)
            return score_signal_expr(signal)

        # ===== 运算 =====
        if isinstance(node, ast.BinOp):
            left = self._parse_node(node.left)
            right = self._parse_node(node.right)

            # 转 Expr
            if not isinstance(left, (int, float)):
                left_expr = left
            else:
                left_expr = left

            if not isinstance(right, (int, float)):
                right_expr = right
            else:
                right_expr = right

            if isinstance(node.op, ast.Add):
                return left_expr + right_expr
            elif isinstance(node.op, ast.Sub):
                return left_expr - right_expr
            elif isinstance(node.op, ast.Mult):
                return left_expr * right_expr
            elif isinstance(node.op, ast.Div):
                return left_expr / right_expr

        raise ValueError(f"Unsupported expression: {ast.dump(node)}")


class ExprSignal(BaseSignal):
    def __init__(self, name: str, expr: BaseExpr):
        self._name = name
        self.expr = expr

    @property
    def name(self):
        return self._name

    def generate(self, data):
        # 执行 expr
        signal_values = {}

        for s in self.expr.signals():
            signal_values[s.name] = s.generate(data)

        result = self.expr.evaluate(signal_values)

        # 转成标准 signal（1 / 0 / -1）
        return result.fillna(0).astype(int)


class SignalRegistry:
    registry = {}

    @classmethod
    def register(cls, name: str, signal_cls):
        cls.registry[name] = signal_cls

    @classmethod
    def create(cls, name: str):
        if name not in cls.registry:
            raise ValueError(f"Unknown signal: {name}")
        return cls.registry[name]()


class SignalBuilder:

    def __init__(self):
        self.expr_parser = ExprParser()

    def build(self, expr_str: str) -> BaseSignal:
        expr_str = expr_str.strip()

        # ===== 简单 signal =====
        if expr_str in SignalRegistry.registry:
            return SignalRegistry.create(expr_str)

        # ===== expression =====
        expr = self.expr_parser.parse(expr_str)

        return ExprSignal(name=expr_str, expr=expr)
