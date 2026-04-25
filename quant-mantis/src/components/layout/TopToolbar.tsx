// components/layout/TopToolbar.tsx
import { useState, useEffect, useRef } from "react"
import { Box, Button, MenuItem, Select, Typography, Tooltip } from "@mui/material"
import MessageBar from "../misc/MessageBar"  // 👈 导入
import { type BacktestConfig, fetchBacktestConfigs } from "../../api/Client"
import { useBacktestStore } from "../../store/backtest/backtest.store"
// import { useStrategyStore } from "../../store/backtest/strategy.store"
// import { useFactorStore } from "../../store/backtest/factor.store"
// import { useSignalStore } from "../../store/backtest/signal.store"

export default function TopToolbar({ runBacktest, launchBacktestWizard }: any) {
    const [configs, setConfigs] = useState<BacktestConfig[]>([])
    const [selectedId, setSelectedId] = useState<string>("")
    const currentConfigIdRef = useRef<string>("")
    const applyBacktestConfigStore = useBacktestStore(s => s.applyBacktestConfig)
    // const originalSnapshot = useBacktestStore(s => s.originalSnapshot)

    const refreshBacktestConfigs = () => {
        fetchBacktestConfigs().then(res => {
            // originalSnapshot
            if (res) setConfigs(res)
        })
    }
    useEffect(() => {
        refreshBacktestConfigs()
    }, [])

    const applyBacktestConfig = (config: BacktestConfig) => {
        // 👉 防止重复
        if (currentConfigIdRef.current === config.id) return
        currentConfigIdRef.current = config.id

        applyBacktestConfigStore(config)
    }

    return (
        <Box
            sx={{
                height: 64,
                display: "flex",
                alignItems: "center",
                px: 2,
                borderBottom: "1px solid #333",
                gap: 2, // 👈 关键：自动给所有按钮加间距
            }}
        >
            <Tooltip title="Run backtest based on current dataset and portfolio config">
                <Button variant="contained" onClick={runBacktest}>
                    🚀 Run
                </Button>
            </Tooltip>

            <Tooltip title="Run backtest with wizard...">
                <Button variant="contained" onClick={launchBacktestWizard}>
                    🧪 Run with Wizard
                </Button>
            </Tooltip>

            <Tooltip title="Select a portfolio config to apply">
                <Select
                    value={selectedId}
                    displayEmpty
                    sx={{
                        minWidth: 300,
                        maxWidth: 600,
                        height: 48          // ⭐ 关键
                    }}
                    renderValue={(selected) => {
                        const c = configs.find(cfg => cfg.id === selected)
                        if (!c) return ""

                        return (
                            <Box sx={{ display: "flex", flexDirection: "column" }}>
                                <Typography
                                    sx={{
                                        fontSize: 16,
                                        lineHeight: 1.2,
                                        fontWeight: 500,
                                        textAlign: "left"
                                    }}
                                >
                                    {c.name}
                                </Typography>
                                <Typography
                                    sx={{
                                        fontSize: 13,
                                        color: "text.secondary",
                                        lineHeight: 1.2,
                                        textAlign: "left",
                                        opacity: 0.7   // 👈 更淡一点（关键）
                                    }}
                                >
                                    ID: {c.id}
                                </Typography>
                            </Box>
                        )
                    }}
                    onOpen={() => refreshBacktestConfigs()}
                    onChange={(e) => {
                        const id = e.target.value
                        // ✅ 正确的防重复
                        if (id === selectedId) return

                        setSelectedId(id)

                        const config = configs.find(c => c.id === id)
                        if (config) {
                            applyBacktestConfig(config)
                        }
                    }}
                >
                    {configs.map(c => (
                        <MenuItem key={c.id} value={c.id}>
                            <Box sx={{ display: "flex", flexDirection: "column" }}>
                                {/* 第一行：name */}
                                <Typography
                                    sx={{
                                        fontSize: 16,
                                        lineHeight: 1.2,
                                        fontWeight: 500,
                                        textAlign: "left"
                                    }}
                                >
                                    {c.id === currentConfigIdRef.current ? (
                                        <Box>
                                            💙 {c.name}
                                        </Box>
                                    ) : (
                                        c.name
                                    )}
                                </Typography>

                                {/* 第二行：id */}
                                <Typography
                                    sx={{
                                        fontSize: 13,
                                        color: "text.secondary",
                                        lineHeight: 1.2,
                                        textAlign: "left",
                                        opacity: 0.7   // 👈 更淡一点（关键）
                                    }}
                                >
                                    ID: {c.id}
                                </Typography>
                            </Box>
                        </MenuItem>
                    ))}
                </Select>
            </Tooltip>

            <Button variant="outlined">
                💾 Save
            </Button>

            {/* 全局消息条（自动靠右） */}
            <MessageBar />
        </Box>
    )
}