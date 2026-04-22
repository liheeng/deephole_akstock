// pages/BacktestPage.tsx

import { Box } from "@mui/material"
import Split from "react-split"

// import { useNodes } from "../../hooks/useNodes"
import { NodeRegistry } from "../model/dsl_node/node_registry";
import TopToolbar from "../components/layout/TopToolbar"
import PortfolioPanel from "../components/portfolio/PortfolioPanel"
import BacktestResult from "../components/backtest/BacktestResult"
import StrategyGraph from "../components/backtest/StrategyGraph"
import { useBacktestStore} from "../store/backtest/backtest.store"
import { useBacktestResultStore } from "../store/backtest/backtestresult.store"
import GlobalDialogs from "../components/dsl/GlobalEditorDialog"
import { callBacktest } from "../api/Client";
import { useDialogStore } from "../store/dialog.store"
import { useDatasetStore } from "../store/dataset.store";
import WindowWrapper from "../components/misc/WindowWrapper"

export default function BacktestPage() {
    const buildPortfolioPayload = useBacktestStore((state) => state.buildPayload)
    const buildBacktestPayload = useDatasetStore((state) => state.buildCurrentDatasetPayload)
    const setBacktestResult = useBacktestResultStore((state) => state.setBacktestResult)
    // const nodes = useNodes()
    const nodes = NodeRegistry.toDict()

    const datasetId = useBacktestStore(s => s.datasetId)
    const datasets = useDatasetStore(s => s.datasets)
    const dataset = datasets.find(d => d.id === datasetId)
        
    const openWizard = useDialogStore((s: any) => s.openDialog)
    
    // ❗防止未加载

    const runBacktest = async () => {
        const portfolioConfig = buildPortfolioPayload()
        const datasetConfig = buildBacktestPayload()
        const backtestConfig = {
            portfolio_config: portfolioConfig,
            dataset_config: datasetConfig,
        }
        const data = await callBacktest(backtestConfig)
        if (data) {
            setBacktestResult(data)
        }
    }

    const launchBacktestWizard = async () => {
        // const payload = buildPayload()
        // const data = await callBacktest(payload)
        // if (data) {
        //     setBacktestResult(data)
        // }
        openWizard("backtest_wizard", {datasetSourceDef: dataset?.sourceDef, runBacktest: runBacktest})
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
                sizes={[70, 30]}
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
                        <BacktestResult runBacktest={runBacktest} />
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
                    <WindowWrapper title="Strategy Graph" defaultMode="normal">
                        <Box sx={{ flex: 1, minHeight: 0, minWidth: 0}}>
                            <StrategyGraph />
                        </Box>
                    </WindowWrapper>
                </Box>

            </Split>

            {GlobalDialogs({nodes})}
        </Box>

    )
}