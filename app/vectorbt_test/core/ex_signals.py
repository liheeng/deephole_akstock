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
        "_ma5_above_ma20",
        lambda name="ma5_above_ma20", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="ma5_above_ma20", group="signal", desc="MA5 above MA20"),
    )

    NodeRegistry.register(
        "_ma20_above_ma60",
        lambda name="ma20_above_ma60", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="ma20_above_ma60", group="signal", desc="MA20 above MA60"),
    )

    NodeRegistry.register(
        "_close_above_ma20",
        lambda name="close_above_ma20", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="close_above_ma20", group="signal", desc="Close above MA20"),
    )

    NodeRegistry.register(
        "_rsi_overbought",
        lambda name="rsi_overbought", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="rsi_overbought", group="signal", desc="RSI overbought"),
    )

    NodeRegistry.register(
        "_rsi_oversold",
        lambda name="rsi_oversold", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="rsi_oversold", group="signal", desc="RSI oversold"),
    )

    NodeRegistry.register(
        "_breakout_20d",
        lambda name="breakout_20d", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="breakout_20d", group="signal", desc="20-day breakout"),
    )

    NodeRegistry.register(
        "_breakdown_20d",
        lambda name="breakdown_20d", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="breakdown_20d", group="signal", desc="20-day breakdown"),
    )

    NodeRegistry.register(
        "_boll_upper_break",
        lambda name="boll_upper_break", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(
            name="boll_upper_break",
            group="signal",
            desc="Break above Bollinger upper band",
        ),
    )

    NodeRegistry.register(
        "_boll_lower_break",
        lambda name="boll_lower_break", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(
            name="boll_lower_break",
            group="signal",
            desc="Break below Bollinger lower band",
        ),
    )

    NodeRegistry.register(
        "_vol_spike",
        lambda name="vol_spike", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="vol_spike", group="signal", desc="Volume spike"),
    )

    NodeRegistry.register(
        "_vol_ma5_above_ma20",
        lambda name="vol_ma5_above_ma20", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(
            name="vol_ma5_above_ma20", group="signal", desc="Volume MA5 above MA20"
        ),
    )

    NodeRegistry.register(
        "_up_3days",
        lambda name="up_3days", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="up_3days", group="signal", desc="Up 3 consecutive days"),
    )

    NodeRegistry.register(
        "_down_3days",
        lambda name="down_3days", signal_group=SignalGroup.TS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="down_3days", group="signal", desc="Down 3 consecutive days"),
    )

    # =========================
    # TS / CS 双用 Signals
    # =========================

    NodeRegistry.register(
        "_atr_high_vol",
        lambda name="atr_high_vol", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="atr_high_vol", group="signal", desc="ATR high volatility"),
    )

    NodeRegistry.register(
        "_low_volatility",
        lambda name="low_volatility", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="low_volatility", group="signal", desc="Low volatility"),
    )

    NodeRegistry.register(
        "_high_volatility",
        lambda name="high_volatility", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="high_volatility", group="signal", desc="High volatility"),
    )

    NodeRegistry.register(
        "_volume_spike",
        lambda name="volume_spike", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="volume_spike", group="signal", desc="Volume spike"),
    )

    NodeRegistry.register(
        "_volume_trend",
        lambda name="volume_trend", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="volume_trend", group="signal", desc="Volume trend"),
    )

    NodeRegistry.register(
        "_breakout_confirm",
        lambda name="breakout_confirm", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="breakout_confirm", group="signal", desc="Breakout confirmation"),
    )

    NodeRegistry.register(
        "_reversal_signal",
        lambda name="reversal_signal", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="reversal_signal", group="signal", desc="Reversal signal"),
    )

    NodeRegistry.register(
        "_momentum_strong",
        lambda name="momentum_strong", signal_group=SignalGroup.TS_CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="momentum_strong", group="signal", desc="Strong momentum"),
    )

    # =========================
    # 更适合 CS Signals
    # =========================

    NodeRegistry.register(
        "_trend_strong",
        lambda name="trend_strong", signal_group=SignalGroup.CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="trend_strong", group="signal", desc="Strong trend"),
    )

    NodeRegistry.register(
        "_trend_weak",
        lambda name="trend_weak", signal_group=SignalGroup.CS: ExSignal(
            name, signal_group
        ),
        NodeMeta(name="trend_weak", group="signal", desc="Weak trend"),
    )
