import { useEffect } from "react"
import {
    Box,
    Button,
    Typography,
    Stack,
    Chip,
    Tooltip,
    Divider,
    Select,
    MenuItem,
    TextField,
    Switch,
    ToggleButtonGroup,
    ToggleButton
} from "@mui/material"

import { useSignalStore } from "../../store/backtest/signal.store"
import { useStrategyStore } from "../../store/backtest/strategy.store"
import { useDialogStore } from "../../store/dialog.store"
import { useBacktestStore } from "../../store/backtest/backtest.store"
import StrategyList from "./StrategyList"
import SignalEditor from "../signal/SignalEditor"
import { NodeRegistry } from "../../model/dsl_node/node_registry"
import BacktestDataPanel from "../backtest/BacktestDataPanel"
import NestedChip from "../misc/NestedChip"
import { useMessageStore } from "../../store/message.store"
import { updateBacktestConfig } from "../../api/Client"

export default function PortfolioPanel() {

    const nodes = NodeRegistry.toDict()
    // Init
    useBacktestStore.getState().applyBacktestConfig(undefined)
    
    // =========================
    // ✅ 精细订阅（核心）
    // =========================
    useEffect(() => {
        // 👇 初始化 snapshot
        useBacktestStore.getState().applyBacktestConfig(undefined)
    }, [])

    const portfolioName = useBacktestStore(s => s.name)
    const setPortfolioName = useBacktestStore(s => s.setPortfolioName)
    const portfolioMode = useBacktestStore(s => s.portfolio_mode)
    const setPortfolioMode = useBacktestStore(s => s.setPortfolioMode)

    const freq = useBacktestStore(s => s.params.freq)
    const initCash = useBacktestStore(s => s.params.init_cash)

    const strategyCount = useStrategyStore(s => s.strategyIds.length)
    const updateStrategyMode = useStrategyStore(s => s.updateStrategyMode)

    const scheduleEnabled = useBacktestStore(s => s.schedule_signal.enabled)
    const scheduleSignalId = useBacktestStore(s => s.schedule_signal.signalId)
    const setScheduleSignal = useBacktestStore(s => s.setScheduleSignal)

    const strategyOp = useBacktestStore(s => s.strategy_op)
    const setStrategyOp = useBacktestStore(s => s.setStrategyOp)

    const voteWeights = useBacktestStore(s => s.vote_weights)
    const setVoteWeights = useBacktestStore(s => s.setVoteWeights)

    const strategyWeights = useBacktestStore(s => s.strategy_weights)
    const setStrategyWeights = useBacktestStore(s => s.setStrategyWeights)

    // const portfolioParams = useBacktestStore(s => s.params)
    const updatePortfolioParams = useBacktestStore(s => s.updatePortfolioParams)
    const addMessage = useMessageStore(state => state.addMessage)

    const setOriginalSnapshot = useBacktestStore(s => s.setOriginalSnapshot)
    const isDirty = useBacktestStore(s => s.isDirty())

    // 👉 从 signal store 取 expr
    const scheduleValue = useSignalStore(s =>
        scheduleSignalId ? s.signals[scheduleSignalId]?.expr || "" : ""
    )

    // 👉 action
    const updateSignal = useSignalStore(s => s.updateSignal)
    const createSignal = useSignalStore(s => s.createSignal)

    const openDialog = useDialogStore(s => s.openDialog)

    // ===== helper =====
    // const parseArray = (v: string) =>
    //     v.split(",").map(x => Number(x.trim())).filter(x => !isNaN(x))
    const validateCurrentPortfolio = useBacktestStore(s => s.validate)

    const handleSave = async () => {

        const result = validateCurrentPortfolio()

        if (!result.isValid()) {
            addMessage("error", result.errors.join(", ") || "Validation current portfolio config failed")
            return
        }

        const res = await updateBacktestConfig(useBacktestStore.getState())

        if (res) {
            setOriginalSnapshot()
            addMessage("success", `Portfolio config "${portfolioName}" saved`)
        } else {
            addMessage("error", `Save portfolio config "${portfolioName}" failed`)
        }
    }


    return (
        <Box>

            {/*================= Data Source ================= */}
            <Box sx={{ mb: 2 }}>
                <BacktestDataPanel />
            </Box>

            <Divider sx={{ mb: 1 }} />

            {/* ================= Header ================= */}
            <Box sx={{ mb: 1 }}>
                {/* BacktestDataPanel */}
                <Box
                    sx={{
                        display: "flex",
                        flexDirection: "row",
                        alignItems: "center",   // ⭐ 核心：垂直居中
                        // justifyContent: "space-between",
                        justifyContent: 'flex-start',
                        mb: 1,
                        minHeight: 40           // ⭐ 关键：统一这一行高度

                    }}
                >

                    <Divider sx={{ mb: 1 }} />
                    <Typography variant="subtitle1" sx={{ textAlign: "left", maxWidth: "50%", width: "200", mb: 1 }}>
                        📂 Portfolio Name:
                    </Typography>
                    <Stack
                        component="div"
                        direction="row"
                        spacing={1}
                        // alignItems="center"   // ⭐ 必须有
                    >
                        <Tooltip title="Edit portfolio name">
                            <TextField
                                size="small"
                                value={portfolioName}
                                sx={{ flex: 1, width: 400, maxWidth: 600, height: 40, paddingLeft: 2 }}
                                onChange={(e) => {
                                    if (e.target.value && e.target.value.trim() == portfolioName) {
                                        return
                                    }

                                    setPortfolioName(e.target.value)
                                }
                                }
                            />
                        </Tooltip>
                        <Tooltip title="Save current portfolio config">
                            <Button
                                disabled={!isDirty}
                                size="small"
                                onClick={handleSave}
                                sx={{
                                    height: 40,          // ⭐ 和 Select 对齐
                                    display: "flex",
                                    alignItems: "center"
                                }}
                            >
                                Save
                            </Button>
                        </Tooltip>
                    </Stack>
                </Box>

                {/* mode */}
                <Box
                    sx={{
                        display: "flex",
                        alignItems: "center",
                        mt: 1,
                        p: 0.5,           // 保持较小的内边距
                        pl: 2,            // 左侧文字留白
                        borderRadius: 10,
                        backgroundColor: "#4d4c4c",
                        border: "1px solid rgba(161, 156, 156, 0.15)",
                        width: "fit-content",
                    }}
                >
                    <Typography
                        variant="body2"
                        sx={{
                            fontWeight: 600,
                            color: "rgba(255,255,255,0.8)",
                            mr: 2,
                            fontSize: "0.75rem",
                            textTransform: "uppercase",
                            letterSpacing: "0.5px"
                        }}
                    >
                        Portfolio Mode
                    </Typography>

                    <ToggleButtonGroup
                        value={portfolioMode}
                        exclusive
                        // ⭐ 注意：onChange 的第二个参数是点击的值
                        onChange={(_, value) => {
                            value && setPortfolioMode(value)
                            const strategyMode = (value === "signal_strategy" ? "ts" : "cs")
                            updateStrategyMode("", strategyMode)
                            addMessage("warning", "Portfolio mode changed to " + value + ", all strategies are changed as " + strategyMode + ".")
                        }}
                        size="small"
                        sx={{
                            backgroundColor: "rgba(0, 0, 0, 0.2)", // 内部背景色，增加层次感
                            borderRadius: 10,
                            p: 0.5,
                            '& .MuiToggleButton-root': {
                                color: "rgba(255,255,255,0.5)",
                                border: "none",
                                px: 2,
                                py: 0.2,
                                borderRadius: 10, // 按钮本身也是圆角
                                fontSize: "0.9rem",
                                fontWeight: 500,
                                textTransform: "none", // 禁用全大写
                                transition: "all 0.2s ease-in-out",
                                '&.Mui-selected': {
                                    backgroundColor: "#1890ff", // 选中时的品牌蓝
                                    color: "#fff",
                                    boxShadow: "0px 2px 4px rgba(0,0,0,0.3)", // 选中项轻微浮起
                                    '&:hover': {
                                        backgroundColor: "#40a9ff",
                                    },
                                },
                                '&:hover': {
                                    backgroundColor: "rgba(255,255,255,0.1)",
                                    color: "#fff",
                                },
                            },
                        }}
                    >
                        <ToggleButton value="signal_strategy">
                            Signal Strategy
                        </ToggleButton>
                        <ToggleButton value="weight_strategy">
                            Weight Strategy
                        </ToggleButton>
                    </ToggleButtonGroup>
                </Box>

                {/* ===== Summary ===== */}
                <Box sx={{ display: "flex", gap: 1, mt: 1, flexWrap: "wrap" }}>
                    <Tooltip title="Strategies">
                        {/* <CustomNestedChip size="small" label={`🧠 ${strategyCount}`} /> */}
                        <NestedChip size="small" label={
                            <>
                                {`🧠 ${strategyCount}`}
                                {/* <NestedChip size="small" label={`🧠 ${strategyCount}`} /> */}
                            </>
                        } />
                    </Tooltip>
                    {/* {/* 📦 *} */}

                    <Tooltip title="Schedule Signal">
                        <Chip
                            size="small"
                            style={{ minWidth: '56px' }}
                            color={scheduleEnabled ? (scheduleValue ? "success" : "warning") : "default"}
                            label={
                                scheduleEnabled
                                    ? (scheduleValue ? `🎯 ${scheduleValue}` : "🎯 Schedule")
                                    : "🎯 None"
                                // ❌ 🔗 
                            }
                        // ⚡
                        />
                    </Tooltip>

                    {portfolioMode === "signal_strategy" && (
                        <>
                            <Tooltip title="Strategy Op">
                                <Chip
                                    size="small"
                                    style={{ minWidth: '56px' }}
                                    label={`🛠️ ${strategyOp.value}`}
                                    color={strategyOp.enabled ? "success" : "default"}
                                />
                            </Tooltip>
                            <Tooltip title="Vote Weights">
                                <Chip
                                    size="small"
                                    style={{ minWidth: '56px' }}
                                    label={voteWeights.value && voteWeights.value.length > 0 ? `⚖️ [${voteWeights.value}]` : "⚖️ None"}
                                    color={voteWeights.enabled ? "success" : "default"}
                                />
                            </Tooltip>
                        </>
                    )}

                    {portfolioMode === "weight_strategy" && (
                        <Tooltip title="Strategy Weights">
                            <Chip
                                size="small"
                                // style={{ minWidth: '80px' }}
                                label={strategyWeights.value && strategyWeights.value.length > 0 ? `⚖️ [${strategyWeights.value}]` : `⚖️ None`}
                                color={strategyWeights.enabled ? "success" : "default"}
                            />
                        </Tooltip>
                    )}

                    <Tooltip title="Frequency">
                        <Chip size="small" style={{ minWidth: '56px' }} label={`🕒 ${freq}`} />
                    </Tooltip>
                    <Tooltip title="Initial Cash">
                        <Chip size="small" label={`💰 ${initCash}`} />
                    </Tooltip>

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
                            openDialog("schedule_signal", { scheduleSignalId, expr: scheduleValue })
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
                            updatePortfolioParams({ init_cash: Number(e.target.value) })
                        }
                    />
                </Stack>

            </Stack>
        </Box>
    )
}