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
    Switch
} from "@mui/material"

import { useSignalStore } from "../../store/backtest/signal.store"
import { useStrategyStore } from "../../store/backtest/strategy.store"
import { useDialogStore } from "../../store/dialog.store"
import { useBacktestStore } from "../../store/backtest/backtest.store"
import StrategyList from "./StrategyList"
import SignalEditor from "../signal/SignalEditor"
import { NodeRegistry } from "../../model/dsl_node/node_registry"

export default function PortfolioPanel() {

    const nodes = NodeRegistry.toDict()

    // =========================
    // ✅ 精细订阅（核心）
    // =========================

    const portfolioMode = useBacktestStore(s => s.portfolio_mode)
    const setPortfolioMode = useBacktestStore(s => s.setPortfolioMode)
    
    const freq = useBacktestStore(s => s.params.freq)
    const initCash = useBacktestStore(s => s.params.init_cash)

    const strategyCount = useStrategyStore(s => s.strategyIds.length)
    
    const scheduleEnabled = useBacktestStore(s => s.schedule_signal.enabled)
    const scheduleSignalId = useBacktestStore(s => s.schedule_signal.signalId)
    const setScheduleSignal = useBacktestStore(s => s.setScheduleSignal)

    const strategyOp = useBacktestStore(s => s.strategy_op)
    const setStrategyOp = useBacktestStore(s => s.setStrategyOp)
    
    const voteWeights = useBacktestStore(s => s.vote_weights)
    const setVoteWeights = useBacktestStore(s => s.setVoteWeights)

    const strategyWeights = useBacktestStore(s => s.strategy_weights)
    const setStrategyWeights = useBacktestStore(s => s.setStrategyWeights)

    const portfolioParams = useBacktestStore(s => s.params)
    const updatePortfolioParams = useBacktestStore(s => s.updatePortfolioParams)    

    // 👉 从 signal store 取 expr
    const scheduleValue = useSignalStore(s =>
        scheduleSignalId ? s.signals[scheduleSignalId]?.expr || "" : ""
    )

    // 👉 action
    const updateSignal = useSignalStore(s => s.updateSignal)
    const createSignal = useSignalStore(s => s.createSignal)

    const openDialog = useDialogStore(s => s.openDialog)

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

                {/* mode */}
                <Box sx={{ display: "flex", gap: 1, mt: 1 }}>
                    <Select
                        size="small"
                        value={portfolioMode}
                        onChange={(e) => setPortfolioMode(e.target.value as any)}
                    >
                        <MenuItem value="signal_strategy">Signal Strategy</MenuItem>
                        <MenuItem value="weight_strategy">Weight Strategy</MenuItem>
                    </Select>
                </Box>

                {/* ===== Summary ===== */}
                <Box sx={{ display: "flex", gap: 1, mt: 1, flexWrap: "wrap" }}>

                    <Chip size="small" label={`📦 ${strategyCount}`} />

                    <Chip size="small" label={`⚙ ${portfolioMode}`} />

                    <Chip
                        size="small"
                        color={scheduleEnabled ? (scheduleValue ? "success" : "warning") : "default"}
                        label={
                            scheduleEnabled
                                ? (scheduleValue ? `⚡ ${scheduleValue}` : "⚡ Schedule")
                                : "❌ Schedule"
                        }
                    />

                    {portfolioMode === "signal_strategy" && (
                        <>
                            <Chip
                                size="small"
                                label={`🔗 ${strategyOp.value}`}
                                color={strategyOp.enabled ? "success" : "default"}
                            />

                            <Chip
                                size="small"
                                label={`🔗 [${voteWeights.value}]`}
                                color={voteWeights.enabled ? "success" : "default"}
                            />
                        </>
                    )}

                    {portfolioMode === "weight_strategy" && (
                        <Chip
                            size="small"
                            label={`🔗 [${strategyWeights.value}]`}
                            color={strategyWeights.enabled ? "success" : "default"}
                        />
                    )}

                    <Chip size="small" label={`🕒 ${freq}`} />
                    <Chip size="small" label={`💰 ${initCash}`} />

                </Box>
            </Box>

            <Divider sx={{ mb: 1 }} />

            {/* ================= Strategy ================= */}
            <Stack spacing={2}>

                {/* ✅ 已经隔离 */}
                <StrategyList />

                {/* ================= Schedule Signal ================= */}
                <Box
                        sx={{
                            gap: 0,
                            position: 'relative',
                            border: '1px solid rgba(255, 255, 255, 0.23)', // 标准 MUI 边框色
                            borderRadius: 1,
                            p: 2,    // 内部间距
                            pt: 2.5,  // 顶部留出空间给标题
                            mt: 2    // 外部间距
                        }}
                    >

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
                        value={scheduleValue}
                        enabled={scheduleEnabled}

                        onChange={(v: string) => {
                            // ✅ 没有 signalId → 先创建
                            if (!scheduleSignalId) {
                                const id = createSignal(v)

                                setScheduleSignal({
                                    signalId: id,
                                    enabled: true
                                })
                            } else {
                                updateSignal(scheduleSignalId, v)
                            }
                        }}

                        onToggle={() =>
                            setScheduleSignal({ enabled: !scheduleEnabled })
                        }

                        onVisual={() =>
                            openDialog("schedule_signal", scheduleSignalId)
                        }

                        nodes={nodes}
                    />
                </Box>

{/* ================= Portfolio Config ================= */}
{portfolioMode === "signal_strategy" && (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>

        {/* ================= Strategy OP ================= */}
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>

            <Switch
                checked={strategyOp.enabled}
                onChange={() =>
                    setStrategyOp({
                        enabled: !strategyOp.enabled
                    })
                }
            />

            <Typography sx={{ width: 180 }}>
                Strategy OP
            </Typography>

            <Select
                size="small"
                value={strategyOp.value}
                disabled={!strategyOp.enabled}
                sx={{ width: 120 }}
                onChange={(e) =>
                    setStrategyOp({
                        value: e.target.value as any
                    })
                }
            >
                <MenuItem value="AND">AND</MenuItem>
                <MenuItem value="OR">OR</MenuItem>
            </Select>

        </Box>

        {/* ================= Vote Weights ================= */}
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>

            <Switch
                checked={voteWeights.enabled}
                onChange={() =>
                    setVoteWeights({
                        enabled: !voteWeights.enabled
                    })
                }
            />

            <Typography sx={{ width: 180 }}>
                Vote Weights
            </Typography>

            <TextField
                size="small"
                value={voteWeights.value.join(",")}
                disabled={!voteWeights.enabled}
                sx={{ flex: 1, maxWidth: 200 }}
                onChange={(e) =>
                    setVoteWeights({
                        value: e.target.value
                            .split(",")
                            .map(x => Number(x.trim()))
                            .filter(x => !isNaN(x))
                    })
                }
            />

        </Box>
    </Box>
)}

{portfolioMode === "weight_strategy" && (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>

        <Switch
            checked={strategyWeights.enabled}
            onChange={() =>
                setStrategyWeights({
                    enabled: !strategyWeights.enabled
                })
            }
        />

        <Typography sx={{ width: 180 }}>
            Strategy Weights
        </Typography>

        <TextField
            size="small"
            value={strategyWeights.value.join(",")}
            disabled={!strategyWeights.enabled}
            sx={{ flex: 1 }}
            onChange={(e) =>
                setStrategyWeights({
                    value: e.target.value
                        .split(",")
                        .map(x => Number(x.trim()))
                        .filter(x => !isNaN(x))
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
                        value={freq}
                        onChange={(e) =>
                            updatePortfolioParams({ freq: e.target.value })
                        }
                    />

                    <TextField
                        size="small"
                        label="Init Cash"
                        type="number"
                        value={initCash}
                        onChange={(e) =>
                            updatePortfolioParams( { init_cash: Number(e.target.value) })
                        }
                    />
                </Stack>

            </Stack>
        </Box>
    )
}