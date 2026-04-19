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



export default function BacktestPage() {
    const buildPayload = useBacktestStore((state) => state.buildPayload)
    const setBacktestResult = useBacktestResultStore((state) => state.setBacktestResult)
    // const nodes = useNodes()
    const nodes = NodeRegistry.toDict()

    // ❗防止未加载

    const runBacktest = async () => {
        const payload = buildPayload()
        const data = await callBacktest(payload)
        if (data) {
            setBacktestResult(data)
        }
    }

    if (!nodes || Object.keys(nodes).length === 0) {
        return <div>Loading nodes...</div>
    }

    return (
        <Box sx={{ height: "100vh", display: "flex", flexDirection: "column" }}>

            {/* Toolbar */}
            <TopToolbar runBacktest={runBacktest} />

            {/* 垂直 Split（上下） */}
            <Split
                direction="vertical"
                sizes={[70, 30]}         // 上70% 下30%
                minSize={200}
                gutterSize={6}
                style={{ flex: 1, display: "flex", flexDirection: "column" }}
            >

                {/* ===== 上半部分（左右 split） ===== */}
                <Split
                    direction="horizontal"
                    sizes={[25, 75]}      // 左25% 右75%
                    minSize={200}
                    gutterSize={6}
                    style={{ display: "flex" }}
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
                            overflow: "hidden"
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
                    <StrategyGraph />
                </Box>

            </Split>

            {GlobalDialogs({nodes})}
        </Box>

    )
}