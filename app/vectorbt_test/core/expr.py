from abc import ABC, abstractmethod


class Expr(ABC):
    def __init__(self, *args):
        self.args = args

    def cache_key(self):
        if not hasattr(self, "args"):
            raise RuntimeError(f"{self} missing args")
        return (
            self.__class__.__name__,
            tuple(
                arg.cache_key() if isinstance(arg, Expr) else arg
                for arg in self.args
            )
        )
    
    def evaluate(self, data, cache, context):
        key = self.cache_key()

        if key in cache:
            return cache[key]

        result = self.compute(data, cache, context)
        cache[key] = result
        return result

    @abstractmethod
    def compute(self, data, cache, context):
        pass

    @abstractmethod
    def nodes(self):
        pass

    # ===== 算术 =====
    def __add__(self, other):
        return AddExpr(self, to_expr(other))

    def __sub__(self, other):
        return SubExpr(self, to_expr(other))

    def __mul__(self, other):
        return MulExpr(self, to_expr(other))

    def __truediv__(self, other):
        return DivExpr(self, to_expr(other))

    # ===== 比较 =====
    def __gt__(self, other):
        return GreaterExpr(self, to_expr(other))

    def __lt__(self, other):
        return LessExpr(self, to_expr(other))

    def __ge__(self, other):
        return GreaterEqualExpr(self, to_expr(other))

    def __le__(self, other):
        return LessEqualExpr(self, to_expr(other))

    def __eq__(self, other):
        return EqualExpr(self, to_expr(other))

    def __ne__(self, other):
        return NotEqualExpr(self, to_expr(other))

    # ===== 布尔 =====
    def __and__(self, other):
        return AndExpr(self, to_expr(other))

    def __or__(self, other):
        return OrExpr(self, to_expr(other))

    def __invert__(self):
        return NotExpr(self)


def to_expr(x):
    if isinstance(x, Expr):
        return x
    return ConstExpr(x)


# =========================
# NodeExpr（叶子）
# =========================
class NodeExpr(Expr):
    def __init__(self, node):
        super().__init__(node) 
        self.node = node

    def nodes(self):
        return [self.node]

    def compute(self, data, cache, context):
        # The cache is managned by the caller(evaluate function)
        # if self.node.name not in cache:
        #     cache[self.node.name] = self.node.compute(data, cache, context)
        # return cache[self.node.name]
        return self.node.compute(data, cache, context)


# =========================
# 常量
# =========================
class ConstExpr(Expr):
    def __init__(self, value):
        super().__init__(value) 
        self.value = value

    def nodes(self):
        return []

    def compute(self, data, cache, context):
        any_series = next(iter(cache.values()))
        return any_series * 0 + self.value


# =========================
# 运算
# =========================
class BinaryExpr(Expr):
    def __init__(self, left, right):
        super().__init__(left, right) 
        self.left = left
        self.right = right

    def nodes(self):
        return list({n.name: n for n in self.left.nodes() + self.right.nodes()}.values())


class AddExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return self.left.evaluate(data, cache, context) + self.right.evaluate(data, cache, context)


class SubExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return self.left.evaluate(data, cache, context) - self.right.evaluate(data, cache, context)


class MulExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return self.left.evaluate(data, cache, context) * self.right.evaluate(data, cache, context)


class DivExpr(BinaryExpr):
    def compute(self, data, cache, context):
        right = self.right.evaluate(data, cache, context)
        return self.left.evaluate(data, cache, context) / right.replace(0, 1e-9)


class GreaterExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (self.left.evaluate(data, cache, context) >
                self.right.evaluate(data, cache, context)).astype(int)


class LessExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (self.left.evaluate(data, cache, context) <
                self.right.evaluate(data, cache, context)).astype(int)


class AndExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (
            self.left.evaluate(data, cache, context).astype(bool)
            & self.right.evaluate(data, cache, context).astype(bool)
        ).astype(int)


class OrExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (
            self.left.evaluate(data, cache, context).astype(bool)
            | self.right.evaluate(data, cache, context).astype(bool)
        ).astype(int)


class NotExpr(Expr):
    def __init__(self, expr):
        super().__init__(expr) 
        self.expr = expr

    def nodes(self):
        return self.expr.nodes()

    def compute(self, data, cache, context):
        return (~self.expr.evaluate(data, cache, context).astype(bool)).astype(int)


class GreaterEqualExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (
            self.left.evaluate(data, cache, context) >=
            self.right.evaluate(data, cache, context)
        ).astype(int)


class LessEqualExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (
            self.left.evaluate(data, cache, context) <=
            self.right.evaluate(data, cache, context)
        ).astype(int)


class EqualExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (
            self.left.evaluate(data, cache, context) ==
            self.right.evaluate(data, cache, context)
        ).astype(int)


class NotEqualExpr(BinaryExpr):
    def compute(self, data, cache, context):
        return (
            self.left.evaluate(data, cache, context) !=
            self.right.evaluate(data, cache, context)
        ).astype(int)
