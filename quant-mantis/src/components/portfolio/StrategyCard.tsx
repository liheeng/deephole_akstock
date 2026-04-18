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

import { useState } from "react"
import FactorList from "../factor/FactorList"
import SignalEditor from "../signal/SignalEditor"
import { useBacktestStore } from "../../store/backtest.store"
import { useNodes } from "../../hooks/useNodes"

export default function StrategyCard({ index, strategy }: any) {
    const [expanded, setExpanded] = useState(true)

    const nodes = useNodes()

    const {
        updateStrategy,
        openDialog,
    } = useBacktestStore()

    //   const strategy_mode = strategy.strategy_mode // ts / cs

    const factorCount = strategy.factors?.length || 0
    const hasSignal = strategy.signal_enabled

    // ===== 更新函数 =====
    const update = (patch: any) => {
        updateStrategy(index, patch)
    }

    return (
        <Card
            sx={{
                p: 1,
                border: expanded ? "1px solid #666" : "1px solid transparent",
                backgroundColor: expanded ? "rgba(255,255,255,0.03)" : "transparent",
            }}
        >
            {/* ===== Header ===== */}
            <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                <Typography variant="subtitle2">
                    {strategy.name || `Strategy ${index + 1}`}
                </Typography>

                <IconButton size="small" onClick={() => setExpanded(!expanded)}>
                    {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                </IconButton>
            </Box>

            {/* ===== 折叠 summary ===== */}
            {!expanded && (
                <Box sx={{ display: "flex", gap: 1, mt: 1, flexWrap: "wrap" }}>
                    <Tooltip title="Number of factors">
                        <Chip size="small" label={`📊 ${factorCount}`} />
                    </Tooltip>

                    <Tooltip title="Signal enabled">
                        <Chip size="small" label={hasSignal ? "⚡ Signal" : "❌ No Signal"} />
                    </Tooltip>

                    {/* {strategy.threshold.enabled && ( */}
                    {strategy.strategy_mode === "ts" && (
                        <Chip size="small" label={`🎯 ${strategy.threshold.value}`} />
                    )}

                    {/* {strategy.top_nenabled && ( */}
                    {strategy.strategy_mode === "cs" && (
                        <Chip size="small" label={`🏆 Top ${strategy.top_n.value}`} />
                    )}
                </Box>
            )}

            {/* ===== 展开内容 ===== */}
            <Collapse in={expanded}>
                <Stack sx={{ spacing: 1, mt: 1 }}>

                    {/* name */}
                    <TextField
                        size="small"
                        label="Strategy Name"
                        value={strategy.name}
                        onChange={(e) =>
                            update({ name: e.target.value })
                        }
                    />

                    {/* factors */}
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
                        {/* factors */}
                        <FactorList strategyIndex={index} />
                    </Box>
                    {/* strategy signal */}
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
                        {/* signal */}
                        <SignalEditor
                            value={strategy.signal}
                            enabled={strategy.signal_enabled}
                            onChange={(v: string) =>
                                update({ signal: v })
                            }
                            onToggle={() =>
                                update({ signal_enabled: !strategy.signal_enabled })
                            }
                            onVisual={() =>
                                openDialog({
                                    type: "signal",
                                    strategyIndex: index
                                })
                            }
                            nodes={nodes}
                        />
                    </Box>

                    {/* threshold */}
                    {strategy.strategy_mode === "ts" && (
                        <TextField
                            size="small"
                            label="Threshold"
                            value={strategy.threshold.value || ""}
                            onChange={(e) =>
                                update({ threshold: Number(e.target.value) })
                            }
                        />
                    )}

                    {/* top_n */}
                    {strategy.strategy_mode === "cs" && (
                        <TextField
                            size="small"
                            label="Top N"
                            value={strategy.top_n.value || ""}
                            onChange={(e) =>
                                update({ top_n: Number(e.target.value) })
                            }
                        />
                    )}

                </Stack>
            </Collapse>
        </Card>
    )
}