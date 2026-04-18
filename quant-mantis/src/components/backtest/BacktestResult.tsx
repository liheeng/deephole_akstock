import { Card, Button, Box } from "@mui/material"
import ReactECharts from "echarts-for-react"
import TradesTable from "./TradesTable"
import { useBacktestStore } from "../../store/backtest.store"
import { useRef, useState } from "react"
import UniDataGrid from "../table/UniDataGrid"

export default function BacktestResult({ runBacktest }: any) {
    const backtestResult = useBacktestStore(state => state.backtestResult)
    const equity = backtestResult?.equity || []
    const stats = backtestResult?.stats || {}
    const trades = backtestResult?.trades || []

    const [topHeight, setTopHeight] = useState(50)
    const containerRef = useRef<HTMLDivElement>(null)
    const isDragging = useRef(false)

    const handleMouseDown = () => {
        isDragging.current = true
        document.body.style.cursor = "row-resize"
    }

    const handleMouseMove = (e: React.MouseEvent) => {
        if (!isDragging.current || !containerRef.current) return
        const rect = containerRef.current.getBoundingClientRect()
        const newHeight = ((e.clientY - rect.top) / rect.height) * 100
        setTopHeight(Math.max(20, Math.min(80, newHeight)))
    }

    const handleMouseUp = () => {
        isDragging.current = false
        document.body.style.cursor = "default"
    }

    // ==================
    // 统计表格：表头居中，内容左对齐
    // ==================
    const statColumns = [
        {
            field: "name",
            headerName: "Indicator",
            flex: 1,
            headerAlign: "center", // 表头居中
            align: "left"          // 内容左对齐
        },
        {
            field: "value",
            headerName: "Value",
            flex: 1,
            headerAlign: "center", // 表头居中
            align: "left"          // 内容左对齐
        }
    ]

    const statRows = Object.entries(stats).map(([k, v], index) => ({
        id: index,
        name: k,
        value: typeof v === "number" ? v.toFixed(4) : String(v),
    }))

    return (
        <Card
            ref={containerRef}
            sx={{
                p: 2,
                height: "100%",
                display: "flex",
                flexDirection: "column",
                boxSizing: "border-box"
            }}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            onMouseLeave={handleMouseUp}
        >
            <Button variant="contained" onClick={runBacktest} sx={{ mb: 2 }}>
                Run Backtest
            </Button>

            {/* 上半部分：左图表 右统计表格 */}
            <Box sx={{
                display: "flex",
                gap: 2,
                height: `${topHeight}%`,
                minHeight: 200
            }}>
                {/* 左侧 2/3 图表 */}
                <Box sx={{ flex: 2, height: "100%" }}>
                    <ReactECharts
                        option={{
                            tooltip: { trigger: "axis" },
                            xAxis: { type: "category", data: equity.map((_, i) => i) },
                            yAxis: { type: "value", scale: true },
                            series: [{
                                name: "Equity",
                                type: "line",
                                smooth: true,
                                showSymbol: false,
                                data: equity
                            }]
                        }}
                        style={{ height: "100%" }}
                    />
                </Box>

                {/* 右侧 1/3 双列表格 */}
                <Box sx={{ flex: 1, height: "100%" }}>
                    <UniDataGrid
                        rows={statRows}
                        columns={statColumns}
                        autoHeight={false}
                        disableRowSelectionOnClick
                        hideFooterPagination
                        hideFooter
                        sx={{
                            fontSize: "11px",
                            ".MuiDataGrid-cell": { py: 0.5 },
                        }}
                    />
                </Box>
            </Box>

            {/* 拖动条 */}
            <Box
                onMouseDown={handleMouseDown}
                sx={{
                    height: 6,
                    bgcolor: "divider",
                    cursor: "row-resize",
                    my: 1,
                    borderRadius: 1,
                    "&:hover": { bgcolor: "primary.main" }
                }}
            />

            {/* 下半部分：交易表格 */}
            <Box sx={{ flex: 1, overflow: "auto" }}>
                <TradesTable trades={trades} />
            </Box>
        </Card>
    )
}