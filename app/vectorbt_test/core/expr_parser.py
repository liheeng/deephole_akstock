import ast
from .expr import NodeExpr, to_expr
from .registry import NodeRegistry
from .function_expr import FUNCTION_REGISTRY
from typing import Any


class ExprParser:

    def parse(self, expr_str: str):
        tree = ast.parse(expr_str, mode="eval")
        return self._parse_node(tree.body)

    # =========================
    # 主解析函数
    # =========================
    def _parse_node(self, node) -> NodeExpr | Any:

        # ===== 常量 =====
        if isinstance(node, ast.Constant):
            return node.value

        # ===== 变量（Node）=====
        if isinstance(node, ast.Name):
            return NodeExpr(NodeRegistry.create(node.id))

        # ===== 算术运算 =====
        if isinstance(node, ast.BinOp):
            left = self._parse_node(node.left)
            right = self._parse_node(node.right)

            if isinstance(node.op, ast.Add):
                return to_expr(left) + to_expr(right)

            elif isinstance(node.op, ast.Sub):
                return to_expr(left) - to_expr(right)

            elif isinstance(node.op, ast.Mult):
                return to_expr(left) * to_expr(right)

            elif isinstance(node.op, ast.Div):
                return to_expr(left) / to_expr(right)

        # ===== 比较运算 =====
        if isinstance(node, ast.Compare):
            left = self._parse_node(node.left)

            for op, comparator in zip(node.ops, node.comparators):
                right = self._parse_node(comparator)

                if isinstance(op, ast.Gt):
                    left = to_expr(left) > to_expr(right)

                elif isinstance(op, ast.Lt):
                    left = to_expr(left) < to_expr(right)

                elif isinstance(op, ast.GtE):
                    left = to_expr(left) >= to_expr(right)

                elif isinstance(op, ast.LtE):
                    left = to_expr(left) <= to_expr(right)

                elif isinstance(op, ast.Eq):
                    left = to_expr(left) == to_expr(right)

                elif isinstance(op, ast.NotEq):
                    left = to_expr(left) != to_expr(right)

            return left

        # ===== 布尔运算 (& |) =====
        if isinstance(node, ast.BoolOp):
            values = [self._parse_node(v) for v in node.values]

            result = values[0]

            for v in values[1:]:
                if isinstance(node.op, ast.And):
                    result = to_expr(result) & to_expr(v)

                elif isinstance(node.op, ast.Or):
                    result = to_expr(result) | to_expr(v)

            return result

        # ===== 位运算（推荐用这个支持 & |）=====
        if isinstance(node, ast.BinOp):
            if isinstance(node.op, ast.BitAnd):
                return to_expr(self._parse_node(node.left)) & to_expr(self._parse_node(node.right))

            elif isinstance(node.op, ast.BitOr):
                return to_expr(self._parse_node(node.left)) | to_expr(self._parse_node(node.right))

        # ===== NOT (~) =====
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Invert):
                return ~to_expr(self._parse_node(node.operand))

        # ===== 函数调用 =====
        if isinstance(node, ast.Call):
            func_name = node.func.id

            if func_name not in FUNCTION_REGISTRY:
                raise ValueError(f"Unknown function: {func_name}")

            args = [self._parse_node(arg) for arg in node.args]

            return FUNCTION_REGISTRY[func_name](*args)

        raise ValueError(f"Unsupported expression: {ast.dump(node)}")
