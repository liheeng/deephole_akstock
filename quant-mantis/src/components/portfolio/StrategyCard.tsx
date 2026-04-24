import {
    Card,
    Box,
    Typography,
    IconButton,
    Collapse,
    TextField,
    Stack,
    Chip,
    Tooltip
} from "@mui/material"

import ExpandMoreIcon from "@mui/icons-material/ExpandMore"
import ExpandLessIcon from "@mui/icons-material/ExpandLess"
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutlined"

import { useState, useCallback } from "react"

import FactorList from "../factor/FactorList"
import SignalEditor from "../signal/SignalEditor"

import { useStrategyStore } from "../../store/backtest/strategy.store"
import { useSignalStore } from "../../store/backtest/signal.store"
import { useDialogStore } from "../../store/dialog.store"

import { NodeRegistry } from "../../model/dsl_node/node_registry"

export default function StrategyCard({ strategyId }: any) {

    const [expanded, setExpanded] = useState(true)
    const nodes = NodeRegistry.toDict()

    // =========================
    // ✅ 细粒度 selector（核心）
    // =========================

    const name = useStrategyStore(s => s.strategies[strategyId]?.name)
    const factorIds = useStrategyStore(
        s => s.strategies[strategyId]?.factorIds
    )
    const factorCount = factorIds?.length ?? 0
    const signalId = useStrategyStore(s => s.strategies[strategyId]?.signalId)
    const config = useStrategyStore(s => s.strategies[strategyId]?.config)
    // const strategyMode = config?.mode

    // =========================
    // actions（稳定引用）
    // =========================

    const updateStrategyMeta = useStrategyStore(s => s.updateStrategyMeta)
    const deleteStrategy = useStrategyStore(s => s.deleteStrategy)
    const setStrategySignal = useStrategyStore(s => s.setStrategySignal)
    const updateStrategyConfig = useStrategyStore(s => s.updateStrategyConfig)

    const signal = useSignalStore(s =>
        signalId ? s.signals[signalId] : null
    )
    const updateSignal = useSignalStore(s => s.updateSignal)
    const createSignal = useSignalStore(s => s.createSignal)

    const openDialog = useDialogStore(s => s.openDialog)

    if (!name) return null

    // =========================
    // handlers
    // =========================

    const handleNameChange = useCallback((e: any) => {
        updateStrategyMeta(strategyId, { name: e.target.value })
    }, [strategyId, updateStrategyMeta])

    const handleDelete = useCallback((e: any) => {
        e.stopPropagation()
        if (window.confirm(`Delete "${name}"?`)) {
            deleteStrategy(strategyId)
        }
    }, [strategyId, name, deleteStrategy])

    const handleSignalChange = useCallback((v: string) => {
        let sid = signalId

        if (!sid) {
            sid = createSignal(v)
            setStrategySignal(strategyId, sid)
        }

        updateSignal(sid, v)
    }, [strategyId, signalId, createSignal, setStrategySignal, updateSignal])

    const handleSignalToggle = useCallback(() => {
        if (!signalId) {
            const sid = createSignal("")
            setStrategySignal(strategyId, sid)
        } else {
            setStrategySignal(strategyId, undefined as any)
        }
    }, [strategyId, signalId, createSignal, setStrategySignal])

    const handleOpenSignalEditor = useCallback(() => {
        openDialog(
            "signal",
            {
                strategyId,
                signalId,
                expr: signal?.expr || ""
            }
        )
    }, [strategyId, signalId, signal, openDialog])

    // =========================
    // render
    // =========================

    return (
        <Card
            sx={{
                p: 1.5,
                border: expanded ? "1px solid #666" : "1px solid rgba(255,255,255,0.1)",
                backgroundColor: expanded ? "rgba(255,255,255,0.03)" : "transparent",
                transition: "all 0.2s ease",
            }}
        >
            {/* ===== Header ===== */}
            <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                    {name}
                </Typography>

                <Stack direction="row" spacing={0.5}>
                    <Tooltip title="Delete Strategy">
                        <IconButton size="small" onClick={handleDelete}>
                            <DeleteOutlineIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>

                    <IconButton size="small" onClick={() => setExpanded(!expanded)}>
                        {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    </IconButton>
                </Stack>
            </Box>

            {/* ===== 折叠 summary ===== */}
            {!expanded && (
                <Box sx={{ display: "flex", gap: 1, mt: 1 }}>
                    <Chip size="small" label={`📊 ${factorCount}`} variant="outlined" />
                    <Chip
                        size="small"
                        label={signalId ? "⚡ Signal" : "❌ No Signal"}
                        variant="outlined"
                    />
                </Box>
            )}

            {/* ===== 展开 ===== */}
            <Collapse in={expanded}>
                <Stack spacing={2} sx={{ mt: 2 }}>

                    {/* name */}
                    <TextField
                        fullWidth
                        size="small"
                        label="Strategy Name"
                        value={name}
                        onChange={handleNameChange}
                    />

                    {/* factors */}
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
                            Factors
                        </Typography>
                        <FactorList strategyId={strategyId} />
                    </Box>

                    {/* signal */}
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
                            Strategy Signal
                        </Typography>

                        <SignalEditor
                            value={signal?.expr || ""}
                            enabled={!!signalId}
                            onChange={handleSignalChange}
                            onToggle={handleSignalToggle}
                            onVisual={handleOpenSignalEditor}
                            nodes={nodes}
                        />
                    </Box>

                    {/* config */}
                    {config?.mode === "ts" && (
                        <TextField
                            size="small"
                            label="Threshold"
                            type="number"
                            value={config.threshold ?? ""}
                            onChange={(e) =>
                                updateStrategyConfig(strategyId, {
                                    threshold: Number(e.target.value)
                                })
                            }
                        />
                    )}

                    {config?.mode === "cs" && (
                        <TextField
                            size="small"
                            label="Top N"
                            type="number"
                            value={config.top_n ?? ""}
                            onChange={(e) =>
                                updateStrategyConfig(strategyId, {
                                    top_n: Number(e.target.value)
                                })
                            }
                        />
                    )}

                </Stack>
            </Collapse>
        </Card>
    )
}