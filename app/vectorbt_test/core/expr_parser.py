import ast
from vectorbt_test.core.registry import NodeRegistry
from vectorbt_test.core.functions import FUNCTION_REGISTRY
from vectorbt_test.core.nodes import Node, to_node   # 👈 用我们刚才定义的


class ExprParser:

    def parse(self, expr_str: str) -> Node:
        tree = ast.parse(expr_str, mode="eval")
        return self._parse_node(tree.body)

    # =========================
    # 主解析函数
    # =========================
    def _parse_node(self, node) -> Node:

        # ===== 常量 =====
        if isinstance(node, ast.Constant):
            return to_node(node.value)

        # ===== 变量（Node）=====
        if isinstance(node, ast.Name):
            return NodeRegistry.create(node.id)

        # ===== 算术运算 =====
        if isinstance(node, ast.BinOp):

            left = self._parse_node(node.left)
            right = self._parse_node(node.right)

            if isinstance(node.op, ast.Add):
                return left + right

            elif isinstance(node.op, ast.Sub):
                return left - right

            elif isinstance(node.op, ast.Mult):
                return left * right

            elif isinstance(node.op, ast.Div):
                return left / right

            elif isinstance(node.op, ast.BitAnd):
                return left & right

            elif isinstance(node.op, ast.BitOr):
                return left | right

        # ===== 比较运算 =====
        if isinstance(node, ast.Compare):

            left = self._parse_node(node.left)

            for op, comparator in zip(node.ops, node.comparators):
                right = self._parse_node(comparator)

                if isinstance(op, ast.Gt):
                    left = left > right

                elif isinstance(op, ast.Lt):
                    left = left < right

                elif isinstance(op, ast.GtE):
                    left = left >= right

                elif isinstance(op, ast.LtE):
                    left = left <= right

                elif isinstance(op, ast.Eq):
                    left = left == right

                elif isinstance(op, ast.NotEq):
                    left = left != right

            return left

        # ===== 布尔运算（and / or）=====
        if isinstance(node, ast.BoolOp):

            values = [self._parse_node(v) for v in node.values]
            result = values[0]

            for v in values[1:]:
                if isinstance(node.op, ast.And):
                    result = result & v   # 👈 注意：用 &

                elif isinstance(node.op, ast.Or):
                    result = result | v

            return result

        # ===== NOT (~) =====
        if isinstance(node, ast.UnaryOp):

            if isinstance(node.op, ast.Invert):
                return ~self._parse_node(node.operand)

        # ===== 函数调用 =====
        if isinstance(node, ast.Call):

            func_name = node.func.id

            if func_name not in FUNCTION_REGISTRY:
                raise ValueError(f"Unknown function: {func_name}")

            args = [self._parse_node(arg) for arg in node.args]

            return FUNCTION_REGISTRY[func_name](*args)

        raise ValueError(f"Unsupported expression: {ast.dump(node)}")
