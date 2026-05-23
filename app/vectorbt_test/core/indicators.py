from vectorbt_test.core.nodes import FeatureNode, NodeType, NodeDType
from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.base import Scope
import pandas as pd
from utils.group_func_registry import GroupFuncReg


class IndicatorResult:
    def __init__(self, data, type_: NodeDType):
        self.data = data
        self.type = type_

    def to_series(self):
        if isinstance(self.data, pd.Series):
            return self.data

        if isinstance(self.data, pd.DataFrame):
            # 🔥 多股票：直接返回 DataFrame（关键）
            return self.data

        raise TypeError(f"Unsupported type: {type(self.data)}")

    def to_frame(self):
        if isinstance(self.data, pd.DataFrame):
            return self.data
        raise TypeError("Not a DataFrame")

    def to_signal(self):
        if self.type == NodeDType.SIGNAL:
            return self.data.astype(bool)
        raise TypeError("Not a signal")

    def __repr__(self):
        return f"IndicatorResult(type={self.type}, shape={self.data.shape})"
    

class Indicator(FeatureNode):
    output_type = NodeDType.NUMERIC  # 默认
    scope = Scope.TS
    dtype = NodeDType.NUMERIC
    
    def __init__(self):
        super().__init__(NodeType.Indicator)

    def compute(self, data: pd.DataFrame, context: PortfolioContext):
        raise NotImplementedError

    def evaluate(self, data, context: PortfolioContext = PortfolioContext(), return_result=False):
        raw = super().evaluate(data, context)
        indicator_result = self._wrap(raw)
        
        if return_result:
            return indicator_result   # 👈 保留类型
    
        # if indicator_result.type == NodeDType.NUMERIC:
        #     return indicator_result.to_series()

        # elif indicator_result.type == NodeDType.FRAME:
        #     return indicator_result.to_frame()

        # elif indicator_result.type == NodeDType.SIGNAL:
        #     return indicator_result.to_signal()
        # else:
        #     return raw
        out = indicator_result.data

        # 🔥 只做语义校验，不做结构转换
        if indicator_result.type == NodeDType.SIGNAL:
            return out.astype(bool)

        return out
    
    def _wrap(self, raw):
        # 自动规范化
        if self.output_type == NodeDType.SIGNAL:
            raw = raw.astype(bool)

        return IndicatorResult(raw, self.output_type)


# =========================
# Indicator Node（支持fallback）
# =========================
class MAIndicator(Indicator):
    output_type = NodeDType.NUMERIC

    def __init__(self, period):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"ma{self.period}"

    def _args(self):
        return [self.period] + super()._args()
    
    def compute(self, data, context: PortfolioContext):
        # fallback
        return self.apply(
            data["close"],
            lambda x: x.rolling(self.period).mean(),
            context
        )


class RSIIndicator(Indicator):
    output_type = NodeDType.NUMERIC

    def __init__(self, period=14):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"rsi{self.period}"

    def _args(self):
        return [self.period]
    
    def compute(self, data, context: PortfolioContext):
        return self.apply(
            data["close"],
            lambda x: self._compute(x, context),
            context)
    
    def _compute(self, x, context: PortfolioContext):
        delta = x.diff()
        gain = delta.clip(lower=0).rolling(self.period).mean()
        loss = (-delta.clip(upper=0)).rolling(self.period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))


class MacdIndicator(Indicator):
    output_type = NodeDType.NUMERIC

    def __init__(self, fast_period=12, slow_period=26, signal_period=9):
        super().__init__()
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

    @property
    def name(self):
        return f"macd{self.fast_period}_{self.slow_period}_{self.signal_period}"

    def _args(self):
        return [self.fast_period, self.slow_period, self.signal_period] + super()._args()

    def compute(self, data, context: PortfolioContext):
        return self.apply(
            data["close"],
            lambda x: self._compute(x, context),
            context
        )
    
    def _compute(self, x, context: PortfolioContext):
        exp1 = x.ewm(span=self.fast_period, adjust=False).mean()
        exp2 = x.ewm(span=self.slow_period, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=self.signal_period, adjust=False).mean()
        return macd - signal


class ATRIndicator(Indicator):
    output_type = NodeDType.NUMERIC

    def __init__(self, period=14):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"atr{self.period}"

    def _args(self):
        return [self.period] + super()._args()

    def compute(self, data, context: PortfolioContext):
        def _atr(df: pd.DataFrame):
            high = df["high"]
            low = df["low"]
            close = df["close"]

            prev_close = close.shift(1)

            tr = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs()
            ], axis=1).max(axis=1)

            return tr.rolling(self.period).mean()

        return self.apply(data, _atr, context)
    

class BollIndicator(Indicator):
    output_type = NodeDType.FRAME

    def __init__(self, period=20):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"boll_mid_{self.period}"

    def _args(self):
        return [self.period] + super()._args()

    def compute(self, data, context: PortfolioContext):
        return self.apply(
            data["close"],
            lambda x: x.rolling(self.period).mean(),
            context
        )


class BollFullIndicator(Indicator):
    output_type = NodeDType.FRAME

    def __init__(self, period=20, n_std=2):
        super().__init__()
        self.period = period
        self.n_std = n_std

    @property
    def name(self):
        return f"boll_{self.period}_{self.n_std}"

    def _args(self):
        return [self.period, self.n_std] + super()._args()

    def compute(self, data, context: PortfolioContext):
        def _boll(x):
            ma = x.rolling(self.period).mean()
            std = x.rolling(self.period).std()
            upper = ma + self.n_std * std
            lower = ma - self.n_std * std

            return pd.DataFrame({
                "mid": ma,
                "upper": upper,
                "lower": lower
            })

        return self.apply(data["close"], _boll, context)
    

class BreakoutIndicator(Indicator):
    output_type = NodeDType.SIGNAL

    def __init__(self, period=20):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"breakout_{self.period}"

    def _args(self):
        return [self.period] + super()._args()

    def compute(self, data, context: PortfolioContext):
        def _breakout(x):
            rolling_max = x.rolling(self.period).max()
            return x > rolling_max.shift(1)

        return self.apply(data["close"], _breakout, context)
    

class BreakoutFullIndicator(Indicator):
    output_type = NodeDType.SIGNAL

    def __init__(self, period=20):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"breakout_full_{self.period}"

    def _args(self):
        return [self.period] + super()._args()

    def compute(self, data, context: PortfolioContext):
        def _breakout(x):
            rolling_max = x.rolling(self.period).max()
            rolling_min = x.rolling(self.period).min()

            return pd.DataFrame({
                "up": x > rolling_max.shift(1),
                "down": x < rolling_min.shift(1)
            })

        return self.apply(data["close"], _breakout, context)
    

class VolumeMAIndicator(Indicator):
    output_type = NodeDType.NUMERIC

    def __init__(self, period=20):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"vol_ma{self.period}"

    def _args(self):
        return [self.period] + super()._args()

    def compute(self, data, context: PortfolioContext):
        return self.apply(
            data["volume"],
            lambda x: x.rolling(self.period).mean(),
            context
        )
    

class VolumeBreakoutIndicator(Indicator):
    output_type = NodeDType.SIGNAL

    def __init__(self, period=20):
        super().__init__()
        self.period = period

    @property
    def name(self):
        return f"vol_breakout_{self.period}"

    def _args(self):
        return [self.period] + super()._args()

    def compute(self, data, context: PortfolioContext):
        def _vol_breakout(x):
            vol_ma = x.rolling(self.period).mean()
            return x > vol_ma

        return self.apply(data["volume"], _vol_breakout, context)
    

@GroupFuncReg.register(group="nodes")
def register_indicators():
    NodeRegistry.register(
        "MA",
        lambda period: MAIndicator(period),
        NodeMeta(
            name="MA",
            group="indicator",
            desc="移动平均线",
            params=[
                NodeParam("period", "int", 5, "周期")
            ]
        )
    )

    NodeRegistry.register(
        "MA5",
        lambda period=5: MAIndicator(period),
        NodeMeta(
            name="MA5",
            group="indicator",
            desc="5日移动平均线"
        )
    )
    NodeRegistry.register(
        "MA20",
        lambda period=20: MAIndicator(period),
        NodeMeta(
            name="MA20",
            group="indicator",
            desc="20日移动平均线"
        )
    )

    NodeRegistry.register(
        "RSI",
        lambda period=14: RSIIndicator(period),
        NodeMeta(
            name="RSI",
            group="indicator",
            desc="相对强弱指数",
            params=[
                NodeParam("period", "int", 14, "周期")
            ]
        ))

    NodeRegistry.register(
        "MACD",
        lambda fast_period=12, slow_period=26, signal_period=9: MacdIndicator(fast_period, slow_period, signal_period),
        NodeMeta(
            name="MACD",
            group="indicator",
            desc="超买超卖指标",
            params=[
                NodeParam("fast_period", "int", 12, "快线周期"),
                NodeParam("slow_period", "int", 26, "慢线周期"),
                NodeParam("signal_period", "int", 9, "信号线周期")
            ]
        ))
    
    NodeRegistry.register(
        "ATR",
        lambda period=14: ATRIndicator(period),
        NodeMeta(
            name="ATR",
            group="indicator",
            desc="ATR",
            params=[
                NodeParam("period", "int", 14, "周期")
            ]
        ))

    NodeRegistry.register(
        "BreakoutFull",
        lambda period=20: BreakoutFullIndicator(period),
        NodeMeta(
            name="BreakoutFull",
            group="indicator",
            desc="BreakoutFull",
            params=[
                NodeParam("period", "int", 20, "周期")
            ]
        ))

    NodeRegistry.register(
        "VolumeMA",
        lambda period=20: VolumeMAIndicator(period),
        NodeMeta(
            name="VolumeMA",
            group="indicator",
            desc="VolumeMA",
            params=[
                NodeParam("period", "int", 20, "周期")
            ]
        ))

    NodeRegistry.register(
        "VolumeBreakout",
        lambda period=20: VolumeBreakoutIndicator(period),
        NodeMeta(
            name="VolumeBreakout",
            group="indicator",
            desc="VolumeBreakout",
            params=[
                NodeParam("period", "int", 20, "周期")
            ]
        ))  
    
    NodeRegistry.register(
        "Breakout",
        lambda period=20: BreakoutIndicator(period),
        NodeMeta(
            name="Breakout",
            group="indicator",
            desc="Breakout",
            params=[
                NodeParam("period", "int", 20, "周期")
            ]
        ))
    
    NodeRegistry.register(
        "BollIndicator",
        lambda period=20: BollIndicator(period),
        NodeMeta(
            name="BollIndicator",
            group="indicator",
            desc="BollIndicator",
            params=[
                NodeParam("period", "int", 20, "周期")
            ]
        )
    )

    NodeRegistry.register(
        "BollFullIndicator",
        lambda period=20, n_std=2: BollFullIndicator(period, n_std),
        NodeMeta(
            name="BollFullIndicator",
            group="indicator",
            desc="BollFullIndicator",
            params=[
                NodeParam("period", "int", 20, "周期")
            ]
        )
    )
    