import { Card, Button, Box } from "@mui/material"
import ReactECharts from "echarts-for-react"
import TradesTable from "./TradesTable"
import { useBacktestResultStore } from "../../store/backtestresult.store"
import { useRef, useState, useMemo } from "react"
import UniDataGrid from "../table/UniDataGrid"
import type { GridColDef } from '@mui/x-data-grid'

export default function BacktestResult({ runBacktest }: any) {

    // ✅ 完全独立订阅（不会互相影响）
    const equity = useBacktestResultStore(s => s.equity)
    const stats = useBacktestResultStore(s => s.stats)
    const trades = useBacktestResultStore(s => s.trades)

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
    // stats → rows（useMemo 防抖）
    // ==================
    const statRows = useMemo(() => {
        return Object.entries(stats).map(([k, v], index) => ({
            id: index,
            name: k,
            value: typeof v === "number" ? v.toFixed(4) : String(v),
        }))
    }, [stats])

    const statColumns: GridColDef<any>[] = useMemo(() => [
        {
            field: "name",
            headerName: "Indicator",
            flex: 1,
            headerAlign: "center",
            align: "left"
        },
        {
            field: "value",
            headerName: "Value",
            flex: 1,
            headerAlign: "center",
            align: "left"
        }
    ], [])

    // ==================
    // chart option（避免重复创建）
    // ==================
    const chartOption = useMemo(() => ({
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
    }), [equity])

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

            {/* 上半 */}
            <Box sx={{
                display: "flex",
                gap: 2,
                height: `${topHeight}%`,
                minHeight: 200
            }}>
                {/* 图表 */}
                <Box sx={{ flex: 2, height: "100%" }}>
                    <ReactECharts
                        option={chartOption}
                        style={{ height: "100%" }}
                    />
                </Box>

                {/* stats */}
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

            {/* trades */}
            <Box sx={{ flex: 1, overflow: "auto" }}>
                <TradesTable trades={trades} />
            </Box>
        </Card>
    )
}