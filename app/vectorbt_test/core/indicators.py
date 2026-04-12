from .nodes import Node, NodeType
from .registry import NodeRegistry, NodeMeta, NodeParam


class Indicator(Node):
    def __init__(self):
        super().__init__(NodeType.Indicator)


# =========================
# Indicator Node（支持fallback）
# =========================
class MAIndicator(Indicator):
    def __init__(self, period):
        self.period = period

    @property
    def name(self):
        return f"ma{self.period}"

    def compute(self, data, cache, context):
        # 优先 DB
        df = context.get("db_df")
        if df is not None and self.name in df.columns:
            return df[self.name]

        # fallback
        return data["close"].rolling(self.period).mean()


class RSIIndicator(Indicator):
    def __init__(self, period=14):
        self.period = period

    @property
    def name(self):
        return f"rsi{self.period}"

    def compute(self, data, cache, context):
        df = context.get("db_df")
        if df is not None and self.name in df.columns:
            return df[self.name]

        delta = data["close"].diff()
        gain = delta.clip(lower=0).rolling(self.period).mean()
        loss = (-delta.clip(upper=0)).rolling(self.period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    

class MacdIndicator(Indicator):
    def __init__(self, fast_period=12, slow_period=26, signal_period=9):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

    @property
    def name(self):
        return f"macd{self.fast_period}_{self.slow_period}_{self.signal_period}"

    def compute(self, data, cache, context):
        df = context.get("db_df")
        if df is not None and self.name in df.columns:
            return df[self.name]

        exp1 = data["close"].ewm(span=self.fast_period, adjust=False).mean()
        exp2 = data["close"].ewm(span=self.slow_period, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=self.signal_period, adjust=False).mean()
        return macd - signal
    

NodeRegistry.register(
    "ma5",
    lambda period=5: MAIndicator(period),
    NodeMeta(
        name="ma",
        group="indicator",
        desc="移动平均线",
        params=[
            NodeParam("period", "int", 5, "周期")
        ]
    )
)
NodeRegistry.register(
    "ma20",
    lambda period=20: MAIndicator(period),
    NodeMeta(
        name="ma",
        group="indicator",
        desc="移动平均线",
        params=[
            NodeParam("period", "int", 20, "周期")
        ]
    )
)
NodeRegistry.register(
    "rsi14",
    lambda period=14: RSIIndicator(period),
    NodeMeta(
        name="rsi",
        group="indicator",
        desc="相对强弱指数",
        params=[
            NodeParam("period", "int", 14, "周期")
        ]
    ))

NodeRegistry.register(
    "macd",
    lambda fast_period=12, slow_period=26, signal_period=9: MacdIndicator(fast_period, slow_period, signal_period),
    NodeMeta(
        name="macd",
        group="indicator",
        desc="超买超卖指标",
        params=[
            NodeParam("fast_period", "int", 12, "快线周期"),
            NodeParam("slow_period", "int", 26, "慢线周期"),
            NodeParam("signal_period", "int", 9, "信号线周期")
        ]
    ))
