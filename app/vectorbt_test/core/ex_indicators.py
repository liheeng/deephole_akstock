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
def register_ex_indicators():
    # 这里注册一些常用的指标，方便在表达式里直接调用
    NodeRegistry.register(
        "ma5",
        lambda: ExIndicator("ma5"),
        NodeMeta(
            name="ma5",
            group="indicator",
            desc="ma5",
        )
    )

    NodeRegistry.register(
        "ma10",
        lambda: ExIndicator("ma10"),
        NodeMeta(
            name="ma10)",
            group="indicator",
            desc="ma10"
        )
    )

    NodeRegistry.register(
        "ma20",
        lambda: ExIndicator("ma20"),
        NodeMeta(
            name="ma20",
            group="indicator",
            desc="ma20"
        )
    )

    NodeRegistry.register(
        "ma60",
        lambda: ExIndicator("ma60"),
        NodeMeta(
            name="ma60",
            group="indicator",
            desc="ma60"
        )
    )

    NodeRegistry.register(
        "ma120",
        lambda: ExIndicator("ma120"),
        NodeMeta(
            name="ma120",
            group="indicator",
            desc="ma120"
        )
    )

    NodeRegistry.register(
        "ema12",
        lambda: ExIndicator("ema12"),
        NodeMeta(
            name="ema12",
            group="indicator",
            desc="ema12"
        )
    )

    NodeRegistry.register(
        "ema26",
        lambda: ExIndicator("ema26"),
        NodeMeta(
            name="ema26",
            group="indicator",
            desc="ema26"
        )
    )

    NodeRegistry.register(
        "macd",
        lambda: ExIndicator("macd"),
        NodeMeta(
            name="macd",
            group="indicator",
            desc="macd"
        )
    )

    NodeRegistry.register(
        "macd_signal",
        lambda: ExIndicator("macd_signal"),
        NodeMeta(
            name="macd_signal",
            group="indicator",
            desc="macd_signal"
        )
    )

    NodeRegistry.register(
        "macd_his",
        lambda: ExIndicator("macd_his"),
        NodeMeta(
            name="macd_his",
            group="indicator",
            desc="macd_his"
        )
    )

    NodeRegistry.register(
        "rsi14",
        lambda: ExIndicator("rsi14"),
        NodeMeta(
            name="rsi14",
            group="indicator",
            desc="rsi14"
        )
    )

    NodeRegistry.register(
        "k",
        lambda: ExIndicator("kdj_k"),
        NodeMeta(
            name="k",
            group="indicator",
            desc="kdj_k"
        )
    )

    NodeRegistry.register(
        "d",
        lambda: ExIndicator("kdj_d"),
        NodeMeta(
            name="d",
            group="indicator",
            desc="kdj_d"
        )
    )

    NodeRegistry.register(
        "j",
        lambda: ExIndicator("kdj_j"),
        NodeMeta(
            name="j",
            group="indicator",
            desc="kdj_j"
        )
    )

    NodeRegistry.register(
        "atr14",
        lambda: ExIndicator("atr14"),
        NodeMeta(
            name="atr14",
            group="indicator",
            desc="atr14"
        )
    )

    NodeRegistry.register(
        "boll_mid",
        lambda: ExIndicator("boll_mid"),
        NodeMeta(
            name="boll_mid",
            group="indicator",
            desc="boll_mid"
        )
    )

    NodeRegistry.register(
        "boll_up",
        lambda: ExIndicator("boll_up"),
        NodeMeta(    
            name="boll_up",
            group="indicator",
            desc="boll_up"
        )
    )

    NodeRegistry.register(
        "boll_down",
        lambda: ExIndicator("boll_down"),
        NodeMeta(
            name="boll_down",
            group="indicator",
            desc="boll_down"
        )
    )

    NodeRegistry.register(
        "vol_ma5",
        lambda: ExIndicator("vol_ma5"),
        NodeMeta(
            name="vol_ma5",
            group="indicator",
            desc="vol_ma5"
        )
    )

    NodeRegistry.register(
        "vol_ma10",
        lambda: ExIndicator("vol_ma10"),
        NodeMeta(
            name="vol_ma10",
            group="indicator",
            desc="vol_ma10"
        )
    )

    NodeRegistry.register(
        "vol_ma20",
        lambda: ExIndicator("vol_ma20"),
        NodeMeta(
            name="vol_ma20",
            group="indicator",
            desc="vol_ma20"
        )
    )

    NodeRegistry.register(
        "obv",
        lambda: ExIndicator("obv"),
        NodeMeta(
            name="obv",
            group="indicator",
            desc="obv"
        )
    )   

    NodeRegistry.register(
        "ret_1d",
        lambda: ExIndicator("ret_1d"),
        NodeMeta(
            name="ret_1d",
            group="indicator",
            desc="ret_1d"
        )
    )

    NodeRegistry.register(
        "ret_5d",
        lambda: ExIndicator("ret_5d"),
        NodeMeta(
            name="ret_5d",
            group="indicator",
            desc="ret_5d"
        )
    )

    NodeRegistry.register(
        "ret_20d",
        lambda: ExIndicator("ret_20d"),
        NodeMeta(
            name="ret_20d",
            group="indicator",
            desc="ret_20d"
        )
    )

    NodeRegistry.register(
        "pct_from_ma20",
        lambda: ExIndicator("pct_from_ma20"),
        NodeMeta(
            name="pct_from_ma20",
            group="indicator",
            desc="pct_from_ma20"
        )
    )