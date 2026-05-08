// pages/BacktestPage.tsx
import { useEffect, useRef, useState } from "react"
import {
    Box, Backdrop,
    CircularProgress,
    Typography
} from "@mui/material"
import Split from "react-split"

// import { useNodes } from "../../hooks/useNodes"
import { NodeRegistry } from "../../model/dsl_node/node_registry";
import TopToolbar from "../../components/layout/TopToolbar"
import PortfolioPanel from "../../components/portfolio/PortfolioPanel"
import StrategyGraph from "../../components/backtest/StrategyGraph"
import { useBacktestStore } from "../../store/backtest/backtest.store"
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store"
import GlobalDialogs from "../../components/dsl/GlobalEditorDialog"
import { callBacktest } from "../../api/Client";
import { useDialogStore } from "../../store/dialog.store"
import { useDatasetStore } from "../../store/dataset.store";
import WindowWrapper from "../../components/misc/WindowWrapper"
import BacktestResult_V2 from "../../components/backtest-result/BacktestResult-v2";

export default function BacktestPage() {
    const buildPortfolioPayload = useBacktestStore((state) => state.buildPayload)
    const buildBacktestPayload = useDatasetStore((state) => state.buildCurrentDatasetPayload)
    const setBacktestResult = useBacktestResultStore((state) => state.setBacktestResult)
    const validatePortfolioConfig = useBacktestStore((state) => state.validate)
    const validateDatasetConfig = useDatasetStore((state) => state.validateDataset)
    // const nodes = useNodes()
    const nodes = NodeRegistry.toDict()

    const datasetId = useDatasetStore(s => s.currentDatasetId)
    const datasets = useDatasetStore(s => s.datasets)
    const dataset = datasets.find(d => d.id === datasetId)

    const openWizard = useDialogStore((s: any) => s.openDialog)

    const [isBacktesting, setIsBacktesting] = useState(false)
    const waitingRef = useRef(true)

    // ❗防止未加载
    const runBacktest = async () => {
        const portfolioCheck = validatePortfolioConfig()
        const datasetCheck = validateDatasetConfig()

        if (!portfolioCheck.isValid()) {
            alert("投资组合配置错误:\n" + portfolioCheck.errors.join("\n"))
            return
        }

        if (!datasetCheck.isValid()) {
            alert("数据集配置错误:\n" + datasetCheck.errors.join("\n"))
            return
        }

        const portfolioConfig = buildPortfolioPayload()
        const datasetConfig = buildBacktestPayload()

        const backtestConfig = {
            portfolio_config: portfolioConfig,
            dataset_config: datasetConfig,
        }

        setIsBacktesting(true)
        waitingRef.current = true

        try {
            // 真正的回测请求
            const backtestPromise = callBacktest(backtestConfig)

            // 循环 timeout 检查
            while (waitingRef.current) {

                const result = await Promise.race([
                    backtestPromise,
                    new Promise((resolve) =>
                        setTimeout(() => resolve("__timeout__"), 30000)
                    )
                ])

                // 回测完成
                if (result !== "__timeout__") {
                    if (result) {
                        setBacktestResult(result)
                    }

                    setIsBacktesting(false)
                    return
                }

                // timeout
                const shouldContinue = window.confirm(
                    "回测仍在运行，是否继续等待 30 秒？"
                )

                if (!shouldContinue) {
                    waitingRef.current = false
                    setIsBacktesting(false)
                    return
                }
            }

        } catch (err) {
            console.error(err)
            alert("回测失败")

        } finally {
            setIsBacktesting(false)
        }
    }

    const launchBacktestWizard = async () => {
        const portfolioCheck = validatePortfolioConfig()
        if (!portfolioCheck.isValid()) {
            alert("投资组合配置错误:\n" + portfolioCheck.errors.join("\n"))
            return
        }

        openWizard("backtest_wizard", { dataset: dataset, runBacktest: runBacktest })
    }

    if (!nodes || Object.keys(nodes).length === 0) {
        return <div>Loading nodes...</div>
    }

    return (
        <Box
            sx={{
                height: "100vh",
                display: "flex",
                flexDirection: "column",
                overflow: "hidden"   // 🔥 防止整体撑开
            }}
        >

            {/* Toolbar */}
            <TopToolbar runBacktest={runBacktest} launchBacktestWizard={launchBacktestWizard} />

            {/* 垂直 Split（上下） */}
            <Split
                direction="vertical"
                sizes={[85, 15]}
                minSize={200}
                gutterSize={6}
                style={{
                    flex: 1,
                    display: "flex",
                    flexDirection: "column",
                    height: "100%",   // 🔥 补
                    minHeight: 0      // 🔥 补
                }}
            >

                {/* ===== 上半部分（左右 split） ===== */}
                <Split
                    direction="horizontal"
                    sizes={[25, 75]}
                    minSize={200}
                    gutterSize={6}
                    style={{
                        display: "flex",
                        height: "100%",   // 🔥 必加
                        minHeight: 0      // 🔥 必加
                    }}
                >

                    {/* LEFT */}
                    <Box
                        sx={{
                            overflow: "auto",
                            borderRight: "1px solid #333",
                            p: 1
                        }}
                    >
                        <PortfolioPanel />
                    </Box>

                    {/* RIGHT */}
                    <Box
                        sx={{
                            display: "flex",
                            flexDirection: "column",
                            overflow: "hidden",
                            minHeight: 0   // 🔥 必加（你之前没加）
                        }}
                    >
                        {/* <BacktestResult runBacktest={runBacktest} /> */}
                        <BacktestResult_V2 />
                    </Box>

                </Split>

                {/* ===== 下半部分（Graph） ===== */}
                <Box
                    sx={{
                        borderTop: "1px solid #333",
                        p: 1,
                        overflow: "hidden"
                    }}
                >
                    <WindowWrapper title="Strategy Graph" defaultMode="normal" disableMinimize={true}>
                        <Box sx={{ flex: 1, minHeight: 0, minWidth: 0 }}>
                            <StrategyGraph />
                        </Box>
                    </WindowWrapper>
                </Box>

            </Split>

            {GlobalDialogs({ nodes })}

            <Backdrop
                open={isBacktesting}
                sx={{
                    color: "#fff",
                    zIndex: 9999,
                    flexDirection: "column",
                    gap: 2
                }}
            >
                <CircularProgress color="inherit" />
                <Typography>
                    Backtesting Running...
                </Typography>
            </Backdrop>
        </Box>

    )
}