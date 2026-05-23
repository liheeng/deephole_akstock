from vectorbt_test.core.registry import NodeRegistry, NodeMeta
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.signals import Signal, SignalGroup
from utils.group_func_registry import GroupFuncReg


class ExSignal(Signal):

    def __init__(self, name: str, signal_group: SignalGroup):
        super().__init__()
        self._name = name
        self.signal_group = signal_group

    def compute(self, data, context: PortfolioContext):
        # fallback

        if context.data_provider is not None:
            return context.data_provider.get(self, data, context)
        return data[self.name]


@GroupFuncReg.register(group="nodes")
def register_ex_signals():
    # =========================
    # 纯 TS Signals
    # =========================

    NodeRegistry.register(
        "ma5_above_ma20",
        lambda: ExSignal("ma5_above_ma20", SignalGroup.TS),
        NodeMeta(name="ma5_above_ma20", group="signal", desc="MA5 above MA20"),
    )

    NodeRegistry.register(
        "ma20_above_ma60",
        lambda: ExSignal("ma20_above_ma60", SignalGroup.TS),
        NodeMeta(name="ma20_above_ma60", group="signal", desc="MA20 above MA60"),
    )

    NodeRegistry.register(
        "close_above_ma20",
        lambda: ExSignal("close_above_ma20", SignalGroup.TS),
        NodeMeta(name="close_above_ma20", group="signal", desc="Close above MA20"),
    )

    NodeRegistry.register(
        "rsi_overbought",
        lambda: ExSignal("rsi_overbought", SignalGroup.TS),
        NodeMeta(name="rsi_overbought", group="signal", desc="RSI overbought"),
    )

    NodeRegistry.register(
        "rsi_oversold",
        lambda: ExSignal("rsi_oversold", SignalGroup.TS),
        NodeMeta(name="rsi_oversold", group="signal", desc="RSI oversold"),
    )

    NodeRegistry.register(
        "breakout_20d",
        lambda: ExSignal("breakout_20d", SignalGroup.TS),
        NodeMeta(name="breakout_20d", group="signal", desc="20-day breakout"),
    )

    NodeRegistry.register(
        "breakdown_20d",
        lambda: ExSignal("breakdown_20d", SignalGroup.TS),
        NodeMeta(name="breakdown_20d", group="signal", desc="20-day breakdown"),
    )

    NodeRegistry.register(
        "boll_upper_break",
        lambda: ExSignal("boll_upper_break", SignalGroup.TS),
        NodeMeta(
            name="boll_upper_break",
            group="signal",
            desc="Break above Bollinger upper band",
        ),
    )

    NodeRegistry.register(
        "boll_lower_break",
        lambda: ExSignal("boll_lower_break", SignalGroup.TS),
        NodeMeta(
            name="boll_lower_break",
            group="signal",
            desc="Break below Bollinger lower band",
        ),
    )

    NodeRegistry.register(
        "vol_spike",
        lambda: ExSignal("vol_spike", SignalGroup.TS),
        NodeMeta(name="vol_spike", group="signal", desc="Volume spike"),
    )

    NodeRegistry.register(
        "vol_ma5_above_ma20",
        lambda: ExSignal("vol_ma5_above_ma20", SignalGroup.TS),
        NodeMeta(
            name="vol_ma5_above_ma20", group="signal", desc="Volume MA5 above MA20"
        ),
    )

    NodeRegistry.register(
        "up_3days",
        lambda: ExSignal("up_3days", SignalGroup.TS),
        NodeMeta(name="up_3days", group="signal", desc="Up 3 consecutive days"),
    )

    NodeRegistry.register(
        "down_3days",
        lambda: ExSignal("down_3days", SignalGroup.TS),
        NodeMeta(name="down_3days", group="signal", desc="Down 3 consecutive days"),
    )

    # =========================
    # TS / CS 双用 Signals
    # =========================

    NodeRegistry.register(
        "atr_high_vol",
        lambda: ExSignal("atr_high_vol", SignalGroup.TS_CS),
        NodeMeta(name="atr_high_vol", group="signal", desc="ATR high volatility"),
    )

    NodeRegistry.register(
        "low_volatility",
        lambda: ExSignal("low_volatility", SignalGroup.TS_CS),
        NodeMeta(name="low_volatility", group="signal", desc="Low volatility"),
    )

    NodeRegistry.register(
        "high_volatility",
        lambda: ExSignal("high_volatility", SignalGroup.TS_CS),
        NodeMeta(name="high_volatility", group="signal", desc="High volatility"),
    )

    NodeRegistry.register(
        "volume_spike",
        lambda: ExSignal("volume_spike", SignalGroup.TS_CS),
        NodeMeta(name="volume_spike", group="signal", desc="Volume spike"),
    )

    NodeRegistry.register(
        "volume_trend",
        lambda: ExSignal("volume_trend", SignalGroup.TS_CS),
        NodeMeta(name="volume_trend", group="signal", desc="Volume trend"),
    )

    NodeRegistry.register(
        "breakout_confirm",
        lambda: ExSignal("breakout_confirm", SignalGroup.TS_CS),
        NodeMeta(name="breakout_confirm", group="signal", desc="Breakout confirmation"),
    )

    NodeRegistry.register(
        "reversal_signal",
        lambda: ExSignal("reversal_signal", SignalGroup.TS_CS),
        NodeMeta(name="reversal_signal", group="signal", desc="Reversal signal"),
    )

    NodeRegistry.register(
        "momentum_strong",
        lambda: ExSignal("momentum_strong", SignalGroup.TS_CS),
        NodeMeta(name="momentum_strong", group="signal", desc="Strong momentum"),
    )

    # =========================
    # 更适合 CS Signals
    # =========================

    NodeRegistry.register(
        "trend_strong",
        lambda: ExSignal("trend_strong", SignalGroup.CS),
        NodeMeta(name="trend_strong", group="signal", desc="Strong trend"),
    )

    NodeRegistry.register(
        "trend_weak",
        lambda: ExSignal("trend_weak", SignalGroup.CS),
        NodeMeta(name="trend_weak", group="signal", desc="Weak trend"),
    )
