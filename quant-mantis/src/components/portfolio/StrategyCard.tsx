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
} from "@mui/material";

import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutlined"; // 引入删除图标

import { useState } from "react";
import FactorList from "../factor/FactorList";
import SignalEditor from "../signal/SignalEditor";
import { useBacktestStore } from "../../store/backtest.store";
import { useNodes } from "../../hooks/useNodes";

export default function StrategyCard({ index, strategy }: any) {
    const [expanded, setExpanded] = useState(true);
    const nodes = useNodes();

    const {
        updateStrategy,
        removeStrategy, // 从 store 中获取删除方法
        openDialog,
    } = useBacktestStore();

    const factorCount = strategy.factors?.length || 0;
    const hasSignal = strategy.signal_enabled;

    // ===== 更新函数 =====
    const update = (patch: any) => {
        updateStrategy(index, patch);
    };

    // 嵌入式边框标题的公用样式组件（内部使用）
    const TitledBox = ({ title, children }: { title: string, children: React.ReactNode }) => (
        <Box
            sx={{
                position: 'relative',
                border: '1px solid rgba(255, 255, 255, 0.23)',
                borderRadius: 1,
                p: 2,
                pt: 2.5,
                mt: 2,
                backgroundColor: 'rgba(0,0,0,0.1)' // 增加一点区分度
            }}
        >
            <Typography
                variant="caption"
                sx={{
                    position: 'absolute',
                    top: -10,
                    left: 12,
                    bgcolor: '#1e1e1e', // ⚠️ 请确保此颜色与你的背景色一致
                    px: 0.5,
                    color: 'text.secondary',
                    fontSize: '0.75rem',
                    fontWeight: 'bold'
                }}
            >
                {title}
            </Typography>
            {children}
        </Box>
    );

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
                <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'primary.light' }}>
                    {strategy.name || `Strategy ${index + 1}`}
                </Typography>

                <Stack direction="row" spacing={0.5}>
                    {/* 删除按钮 */}
                    <Tooltip title="Delete Strategy">
                        <IconButton 
                            size="small" 
                            onClick={(e) => {
                                e.stopPropagation();
                                if (window.confirm(`Delete "${strategy.name || 'this strategy'}"?`)) {
                                    removeStrategy(index);
                                }
                            }}
                            sx={{ 
                                color: 'text.secondary',
                                '&:hover': { color: 'error.main', bgcolor: 'rgba(211, 47, 47, 0.1)' }
                            }}
                        >
                            <DeleteOutlineIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>

                    {/* 折叠按钮 */}
                    <IconButton size="small" onClick={() => setExpanded(!expanded)}>
                        {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    </IconButton>
                </Stack>
            </Box>

            {/* ===== 折叠 summary (仅在关闭时显示) ===== */}
            {!expanded && (
                <Box sx={{ display: "flex", gap: 1, mt: 1, flexWrap: "wrap" }}>
                    <Tooltip title="Number of factors">
                        <Chip size="small" label={`📊 ${factorCount}`} variant="outlined" />
                    </Tooltip>
                    <Tooltip title="Signal status">
                        <Chip 
                            size="small" 
                            label={hasSignal ? "⚡ Signal" : "❌ No Signal"} 
                            color={hasSignal ? "primary" : "default"}
                            variant="outlined"
                        />
                    </Tooltip>
                    {strategy.strategy_mode === "ts" && (
                        <Chip size="small" label={`🎯 ${strategy.threshold?.value || 0}`} variant="outlined" />
                    )}
                    {strategy.strategy_mode === "cs" && (
                        <Chip size="small" label={`🏆 Top ${strategy.top_n?.value || 0}`} variant="outlined" />
                    )}
                </Box>
            )}

            {/* ===== 展开内容 ===== */}
            <Collapse in={expanded}>
                <Stack spacing={2} sx={{ mt: 2 }}>
                    
                    {/* 1. Strategy Name */}
                    <TextField
                        fullWidth
                        size="small"
                        label="Strategy Name"
                        value={strategy.name || ""}
                        onChange={(e) => update({ name: e.target.value })}
                    />

                    {/* 2. Factors Section */}
                    <TitledBox title="Factors">
                        <FactorList strategyIndex={index} />
                    </TitledBox>

                    {/* 3. Strategy Signal Section */}
                    <TitledBox title="Strategy Signal">
                        <SignalEditor
                            value={strategy.signal.value || ""}
                            enabled={strategy.signal_enabled}
                            onChange={(v: string) => update({ signal: v })}
                            onToggle={() => update({ signal_enabled: !strategy.signal_enabled })}
                            onVisual={() =>
                                openDialog({
                                    type: "signal",
                                    strategyIndex: index
                                })
                            }
                            nodes={nodes}
                        />
                    </TitledBox>

                    {/* 4. Mode Specific Settings */}
                    <Box sx={{ mt: 1 }}>
                        {strategy.strategy_mode === "ts" && (
                            <TextField
                                fullWidth
                                size="small"
                                label="Threshold"
                                type="number"
                                value={strategy.threshold?.value ?? ""}
                                onChange={(e) =>
                                    update({ threshold: { ...strategy.threshold, value: Number(e.target.value) } })
                                }
                            />
                        )}

                        {strategy.strategy_mode === "cs" && (
                            <TextField
                                fullWidth
                                size="small"
                                label="Top N"
                                type="number"
                                value={strategy.top_n?.value ?? ""}
                                onChange={(e) =>
                                    update({ top_n: { ...strategy.top_n, value: Number(e.target.value) } })
                                }
                            />
                        )}
                    </Box>
                </Stack>
            </Collapse>
        </Card>
    );
}