import {
    Box,
    Typography,
    Stack,
    Chip,
    Tooltip,
    Divider,
    Select,
    MenuItem,
    TextField,
    Switch,
    // FormControlLabel
} from "@mui/material"

import { useBacktestStore } from "../../store/backtest.store"
import StrategyList from "./StrategyList"
import SignalEditor from "../signal/SignalEditor"
import { useNodes } from "../../hooks/useNodes"

export default function PortfolioPanel() {
    const nodes = useNodes()

    const {
        portfolio_mode,
        setPortfolioMode,

        strategies,

        params,
        schedule_signal,
        strategy_op,
        vote_weights,
        strategy_weights,

        // updateStrategy,
        openDialog,

        setScheduleSignal
    } = useBacktestStore()

    const mode = portfolio_mode

    // ===== helper =====
    const parseArray = (v: string) =>
        v.split(",").map(x => Number(x.trim())).filter(x => !isNaN(x))

    return (
        <Box>

            {/* ================= Header ================= */}
            <Box sx={{ mb: 1 }}>

                <Typography variant="subtitle1">
                    Portfolio
                </Typography>

                {/* ===== mode switch ===== */}
                <Box sx={{ display: "flex", gap: 1, mt: 1 }}>
                    <Select
                        size="small"
                        value={mode}
                        onChange={(e) => setPortfolioMode(e.target.value as any)}
                    >
                        <MenuItem value="signal_strategy">Signal Strategy</MenuItem>
                        <MenuItem value="weight_strategy">Weight Strategy</MenuItem>
                    </Select>
                </Box>

                {/* ===== Summary ===== */}
                <Box
                    sx={{
                        display: "flex",
                        gap: 1,
                        mt: 1,
                        flexWrap: "wrap"
                    }}
                >

                    <Tooltip title="Number of strategies">
                        <Chip size="small" label={`📦 ${strategies.length}`} />
                    </Tooltip>

                    <Tooltip title="Mode">
                        <Chip size="small" label={`⚙ ${mode}`} />
                    </Tooltip>

                    <Tooltip title={schedule_signal.value ? schedule_signal.value : "Schedule Signal"}>
                        <Chip
                            size="small"
                            color={schedule_signal.enabled ? (schedule_signal.value ? "success" : "warning") : 
                                "default"}
                            label={schedule_signal.enabled ? (schedule_signal.value ? `⚡ Schedule Signal: ${schedule_signal.value}` : "⚡ Schedule Signal") 
                                : "❌ Schedule Signal"}
                        />
                    </Tooltip>

                    {/* {mode === "signal_strategy" && strategy_op.enabled && ( */}
                    {mode === "signal_strategy" 
                        && (
                            <Tooltip title="Vote Weights">
                                <Chip 
                                    size="small" 
                                    color={
                                        vote_weights.enabled 
                                            ? (vote_weights.value.length > 0 ? "success" :"warning")
                                            : "default"
                                    }
                                    label={vote_weights.enabled ? `🔗 Vote Weights: [${vote_weights.value}]` : '🔗 Vote Weights'} />
                            </Tooltip>
                        )
                    }
                    {mode === "signal_strategy" 
                        && (
                            <Tooltip title="Strategy Operation">
                                <Chip 
                                    size="small" 
                                    color={
                                        strategy_op.enabled 
                                            ? (strategy_op.value === "OR" ? "success" : "warning") 
                                            : "default"
                                    }
                                    label={`🔗 Strategy OP: ${strategy_op.value}`} />
                            </Tooltip>
                        )
                    }

                    {mode === "weight_strategy" 
                        && (
                            <Tooltip title="Strategy Weights">
                                <Chip 
                                    size="small" 
                                    color={
                                        strategy_weights.enabled 
                                            ? (strategy_weights.value.length > 0 ? "success" :"warning")
                                            : "default"
                                    }
                                    label={strategy_weights.enabled ? `🔗 Strategy Weights: [${strategy_weights.value}]` : '🔗 Strategy Weights'} />
                            </Tooltip>
                        )
                    }

                    <Tooltip title="Frequency">
                        <Chip size="small" label={`🕒 ${params.freq}`} />
                    </Tooltip>

                    <Tooltip title="Initial Cash">
                        <Chip size="small" label={`💰 ${params.init_cash}`} />
                    </Tooltip>

                </Box>
            </Box>

            <Divider sx={{ mb: 1 }} />

            {/* ================= Strategy ================= */}
            <Stack spacing={2}>

                <StrategyList />

                {/* ================= Schedule Signal ================= */}
                {/* <SignalEditor
                    value={schedule_signal.value}
                    enabled={schedule_signal.enabled}
                    onChange={(v: string) =>
                        setScheduleSignal({ ...schedule_signal, value: v })
                    }
                    onToggle={() =>
                        setScheduleSignal({
                            ...schedule_signal,
                            enabled: !schedule_signal.enabled
                        })
                    }
                    onVisual={() =>
                        openDialog({
                            open: true,
                            type: "schedule_signal"
                        })
                    }
                    nodes={nodes}
                /> */}

                <Box
                    sx={{
                        position: 'relative',
                        border: '1px solid rgba(255, 255, 255, 0.23)', // 标准 MUI 边框色
                        borderRadius: 1,
                        p: 2,    // 内部间距
                        pt: 2.5,  // 顶部留出空间给标题
                        mt: 2    // 外部间距
                    }}
                >
                    {/* 标题部分 */}
                    <Typography
                        variant="caption"
                        sx={{
                            position: 'absolute',
                            top: -10,        // 向上偏移一半
                            left: 12,        // 左右边距
                            bgcolor: '#1e1e1e', // 必须设置为与背景相同的颜色，用于遮挡后面的边框线
                            px: 0.5,         // 文字左右的小垫片
                            color: 'text.secondary',
                            fontSize: '0.75rem'
                        }}
                    >
                        Schedule Signal
                    </Typography>

                    <SignalEditor
                        value={schedule_signal.value}
                        enabled={schedule_signal.enabled}
                        onChange={(v: string) => setScheduleSignal({ ...schedule_signal, value: v })}
                        onToggle={() => setScheduleSignal({ ...schedule_signal, enabled: !schedule_signal.enabled })}
                        onVisual={() => openDialog({ open: true, type: "schedule_signal" })}
                        nodes={nodes}
                    />
                </Box>

                {/* ================= Signal Mode Config ================= */}
                {mode === "signal_strategy" && (
                <Stack spacing={1}>

                    {/* ===== Strategy OP ===== */}
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>

                    <Switch
                        checked={strategy_op.enabled}
                        onChange={() =>
                        useBacktestStore.setState({
                            strategy_op: {
                                ...strategy_op,
                                enabled: !strategy_op.enabled
                            }
                        })
                        }
                    />

                    <Typography sx={{ width: 180 }}>
                        Enable Strategy OP
                    </Typography>

                    <Select
                        size="small"
                        value={strategy_op.value}
                        disabled={!strategy_op.enabled}   // ⭐关键
                        sx={{ width: 120 }}
                        onChange={(e) =>
                            useBacktestStore.setState({
                                strategy_op: {
                                    ...strategy_op,
                                    value: e.target.value as any
                                }
                            })
                        }
                    >
                        <MenuItem value="AND">AND</MenuItem>
                        <MenuItem value="OR">OR</MenuItem>
                    </Select>

                    </Box>

                    {/* ===== Vote Weights ===== */}
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>

                    <Switch
                        checked={vote_weights.enabled}
                        onChange={() =>
                            useBacktestStore.setState({
                                vote_weights: {
                                    ...vote_weights,
                                    enabled: !vote_weights.enabled
                                }
                        })
                        }
                    />

                    <Typography sx={{ width: 180 }}>
                        Enable Vote Weights
                    </Typography>

                    <TextField
                        size="small"
                        placeholder="0.5,0.5"
                        value={vote_weights.value.join(",")}
                        disabled={!vote_weights.enabled}   // ⭐关键
                        sx={{ flex: 1, maxWidth: 200 }}
                        onChange={(e) =>
                            useBacktestStore.setState({
                                vote_weights: {
                                    ...vote_weights,
                                    value: parseArray(e.target.value)
                                }
                            })
                        }
                    />

                    </Box>

                </Stack>
                )}

                {/* ================= Weight Mode Config ================= */}
                {mode === "weight_strategy" && (
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>

                    <Switch
                    checked={strategy_weights.enabled}
                    onChange={() =>
                        useBacktestStore.setState({
                        strategy_weights: {
                            ...strategy_weights,
                            enabled: !strategy_weights.enabled
                        }
                        })
                    }
                    />

                    <Typography sx={{ width: 180 }}>
                    Enable Strategy Weights
                    </Typography>

                    <TextField
                    size="small"
                    placeholder="0.3,0.7"
                    value={strategy_weights.value.join(",")}
                    disabled={!strategy_weights.enabled}   // ⭐关键
                    sx={{ flex: 1 }}
                    onChange={(e) =>
                        useBacktestStore.setState({
                        strategy_weights: {
                            ...strategy_weights,
                            value: parseArray(e.target.value)
                        }
                        })
                    }
                    />

                </Box>
                )}

                {/* ================= Params ================= */}
                <Stack direction="row" spacing={1}>

                    <TextField
                        size="small"
                        label="Freq"
                        value={params.freq}
                        onChange={(e) =>
                            useBacktestStore.setState({
                                params: { ...params, freq: e.target.value }
                            })
                        }
                    />

                    <TextField
                        size="small"
                        label="Init Cash"
                        type="number"
                        value={params.init_cash}
                        onChange={(e) =>
                            useBacktestStore.setState({
                                params: { ...params, init_cash: Number(e.target.value) }
                            })
                        }
                    />

                </Stack>

            </Stack>
        </Box>
    )
}