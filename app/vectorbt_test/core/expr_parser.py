import ast
from vectorbt_test.core.registry import NodeRegistry
from vectorbt_test.core.nodes import Node, ConstNode, NodeDType, to_node


class ExprParser:

    def parse(self, expr_str: str) -> Node:
        tree = ast.parse(expr_str, mode="eval")
        return self._parse_node(tree.body)

    # =========================
    # utils
    # =========================
    def _unwrap(self, x):
        """把 ConstNode → 原始值，同时防止 Node 混入 literal 参数"""
        if isinstance(x, ConstNode):
            return x.value

        if isinstance(x, (list, tuple)):
            return type(x)(self._unwrap(v) for v in x)

        if isinstance(x, Node):
            raise ValueError(f"Expected literal, got Node: {x}")

        return x

    def _is_node_param(self, param):
        return param.type in {"Node", "Signal", "Factor", "Indicator", "Data"}

    # =========================
    # 主递归解析
    # =========================
    def _parse_node(self, node) -> Node:

        # ===== 常量 =====
        if isinstance(node, ast.Constant):
            return to_node(node.value)

        # ===== 变量（NodeRegistry）=====
        if isinstance(node, ast.Name):
            return NodeRegistry.create(node.id)

        # ===== 算术 / 位运算 =====
        if isinstance(node, ast.BinOp):
            left = self._parse_node(node.left)
            right = self._parse_node(node.right)
            assert left.dtype == NodeDType.NUMERIC
            assert right.dtype == NodeDType.NUMERIC

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

        # ===== 比较 =====
        if isinstance(node, ast.Compare):
            left_node = self._parse_node(node.left)
            result: Node = None

            for op, comparator in zip(node.ops, node.comparators):
                right_node = self._parse_node(comparator)

                if isinstance(op, ast.Gt):
                    cond = left_node > right_node
                elif isinstance(op, ast.Lt):
                    cond = left_node < right_node
                elif isinstance(op, ast.GtE):
                    cond = left_node >= right_node
                elif isinstance(op, ast.LtE):
                    cond = left_node <= right_node
                elif isinstance(op, ast.Eq):
                    cond = left_node == right_node
                elif isinstance(op, ast.NotEq):
                    cond = left_node != right_node

                result = cond if result is None else (result & cond)
                left_node = right_node

            return result

        # ===== 布尔（and / or）=====
        if isinstance(node, ast.BoolOp):
            values = [self._parse_node(v) for v in node.values]
            result = values[0]
            assert result.dtype in {NodeDType.BOOL, NodeDType.SIGNAL}

            for v in values[1:]:
                assert v in {NodeDType.BOOL, NodeDType.SIGNAL}
                if isinstance(node.op, ast.And):
                    result = result & v
                elif isinstance(node.op, ast.Or):
                    result = result | v

            return result

        # ===== NOT (~) =====
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Invert):
                n = self._parse_node(node.operand)
                assert n.dtype in {NodeDType.BOOL, NodeDType.SIGNAL}
                return ~n

        # ===== 函数调用（核心）=====
        if isinstance(node, ast.Call):

            func_name = node.func.id

            if func_name not in NodeRegistry._meta:
                raise ValueError(f"{func_name} not registered")

            meta = NodeRegistry._meta[func_name]
            param_defs = meta.params or []

            # ===== 解析参数 =====
            parsed_args = [self._parse_node(arg) for arg in node.args]
            parsed_kwargs = {
                kw.arg: self._parse_node(kw.value)
                for kw in node.keywords
            }

            final_kwargs = {}

            for i, param in enumerate(param_defs):

                # ===== 取值（位置优先）=====
                if i < len(parsed_args):
                    val = parsed_args[i]
                elif param.name in parsed_kwargs:
                    val = parsed_kwargs[param.name]
                else:
                    val = param.default

                if val is None:
                    raise ValueError(f"{func_name}: missing param '{param.name}'")

                # ===== 类型处理 =====
                if self._is_node_param(param):

                    if not isinstance(val, Node):
                        raise ValueError(
                            f"{func_name}: param '{param.name}' expects Node, got {val}"
                        )

                    final_kwargs[param.name] = val

                else:
                    raw = self._unwrap(val)

                    if param.type == "int":
                        final_kwargs[param.name] = int(raw)

                    elif param.type == "float":
                        final_kwargs[param.name] = float(raw)

                    elif param.type == "str":
                        final_kwargs[param.name] = str(raw)

                    else:
                        final_kwargs[param.name] = raw

            return NodeRegistry.create(func_name, **final_kwargs)

        raise ValueError(f"Unsupported expression: {ast.dump(node)}")