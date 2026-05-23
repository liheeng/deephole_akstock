from vectorbt_test.core.nodes import NodeDType
from vectorbt_test.core.registry import NodeRegistry, NodeMeta
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.indicators import Indicator
from utils.group_func_registry import GroupFuncReg


# =========================
# Indicator Node（支持fallback）
# =========================
class ExIndicator(Indicator):
    output_type = NodeDType.NUMERIC

    def __init__(self, name):
        super().__init__()
        self._name = name    

    @property
    def name(self):
        return self._name
        # ===== cache key =====
    
    def compute(self, data, context: PortfolioContext):
        # fallback
        if context.data_provider is not None:
            return context.data_provider.get(self, data, context)
        return data[self.name]


@GroupFuncReg.register(group="nodes")
def registry_ex_indicators():
    # 这里注册一些常用的指标，方便在表达式里直接调用
    NodeRegistry.register(
        "_ma5",
        lambda name="ma5": ExIndicator(name),
        NodeMeta(
            name="ma5",
            group="indicator",
            desc="ma5"
        )
    )

    NodeRegistry.register(
        "_ma10",
        lambda name="ma10": ExIndicator(name),
        NodeMeta(
            name="ma10)",
            group="indicator",
            desc="ma10"
        )
    )

    NodeRegistry.register(
        "_ma20",
        lambda name="ma20": ExIndicator(name),
        NodeMeta(
            name="ma20",
            group="indicator",
            desc="ma20"
        )
    )

    NodeRegistry.register(
        "_ma60",
        lambda name="ma60": ExIndicator(name),
        NodeMeta(
            name="ma60",
            group="indicator",
            desc="ma60"
        )
    )

    NodeRegistry.register(
        "_ma120",
        lambda name="ma120": ExIndicator(name),
        NodeMeta(
            name="ma120",
            group="indicator",
            desc="ma120"
        )
    )

    NodeRegistry.register(
        "_ema12",
        lambda name="ema12": ExIndicator(name),
        NodeMeta(
            name="ema12",
            group="indicator",
            desc="ema12"
        )
    )

    NodeRegistry.register(
        "_ema26",
        lambda name="ema26": ExIndicator(name),
        NodeMeta(
            name="ema26",
            group="indicator",
            desc="ema26"
        )
    )

    NodeRegistry.register(
        "_macd",
        lambda name="macd": ExIndicator(name),
        NodeMeta(
            name="macd",
            group="indicator",
            desc="macd"
        )
    )

    NodeRegistry.register(
        "_macd_signal",
        lambda name="macd_signal": ExIndicator(name),
        NodeMeta(
            name="macd_signal",
            group="indicator",
            desc="macd_signal"
        )
    )

    NodeRegistry.register(
        "_macd_his",
        lambda name="macd_his": ExIndicator(name),
        NodeMeta(
            name="macd_his",
            group="indicator",
            desc="macd_his"
        )
    )

    NodeRegistry.register(
        "_rsi14",
        lambda name="rsi14": ExIndicator(name),
        NodeMeta(
            name="rsi14",
            group="indicator",
            desc="rsi14"
        )
    )

    NodeRegistry.register(
        "_kdj_k",
        lambda name="kdj_k": ExIndicator(name),
        NodeMeta(
            name="kdj_k",
            group="indicator",
            desc="kdj_k"
        )
    )

    NodeRegistry.register(
        "_kdj_d",
        lambda name="kdj_d": ExIndicator(name),
        NodeMeta(
            name="kdj_d",
            group="indicator",
            desc="kdj_d"
        )
    )

    NodeRegistry.register(
        "_kdj_j",
        lambda name="kdj_j": ExIndicator(name),
        NodeMeta(
            name="kdj_j",
            group="indicator",
            desc="kdj_j"
        )
    )

    NodeRegistry.register(
        "_atr14",
        lambda name="atr14": ExIndicator(name),
        NodeMeta(
            name="atr14",
            group="indicator",
            desc="atr14"
        )
    )

    NodeRegistry.register(
        "_boll_mid",
        lambda name="boll_mid": ExIndicator(name),
        NodeMeta(
            name="boll_mid",
            group="indicator",
            desc="boll_mid"
        )
    )

    NodeRegistry.register(
        "_boll_up",
        lambda name="boll_up": ExIndicator(name),
        NodeMeta(    
            name="boll_up",
            group="indicator",
            desc="boll_up"
        )
    )

    NodeRegistry.register(
        "_boll_down",
        lambda name="boll_down": ExIndicator(name),
        NodeMeta(
            name="boll_down",
            group="indicator",
            desc="boll_down"
        )
    )

    NodeRegistry.register(
        "_vol_ma5",
        lambda name="vol_ma5": ExIndicator(name),
        NodeMeta(
            name="vol_ma5",
            group="indicator",
            desc="vol_ma5"
        )
    )

    NodeRegistry.register(
        "_vol_ma10",
        lambda name="vol_ma10": ExIndicator(name),
        NodeMeta(
            name="vol_ma10",
            group="indicator",
            desc="vol_ma10"
        )
    )

    NodeRegistry.register(
        "_vol_ma20",
        lambda name="vol_ma20": ExIndicator(name),
        NodeMeta(
            name="vol_ma20",
            group="indicator",
            desc="vol_ma20"
        )
    )

    NodeRegistry.register(
        "_obv",
        lambda name="obv": ExIndicator(name),
        NodeMeta(
            name="obv",
            group="indicator",
            desc="obv"
        )
    )   

    NodeRegistry.register(
        "_ret_1d",
        lambda name="ret_1d": ExIndicator(name),
        NodeMeta(
            name="ret_1d",
            group="indicator",
            desc="ret_1d"
        )
    )

    NodeRegistry.register(
        "_ret_5d",
        lambda name="ret_5d": ExIndicator(name),
        NodeMeta(
            name="ret_5d",
            group="indicator",
            desc="ret_5d"
        )
    )

    NodeRegistry.register(
        "_ret_20d",
        lambda name="ret_20d": ExIndicator(name),
        NodeMeta(
            name="ret_20d",
            group="indicator",
            desc="ret_20d"
        )
    )

    NodeRegistry.register(
        "_pct_from_ma20",
        lambda name="pct_from_ma20": ExIndicator(name),
        NodeMeta(
            name="pct_from_ma20",
            group="indicator",
            desc="pct_from_ma20"
        )
    )