import { Card, Button, Box } from "@mui/material"
import ReactECharts from "echarts-for-react"
import TradesTable from "./TradesTable"
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store"
import { useRef, useState, useMemo } from "react"
import UniDataGrid from "../table/UniDataGrid"
import type { GridColDef } from '@mui/x-data-grid'
import { Tabs, Tab } from "@mui/material"
import { FullScreenBox } from "../misc//FullScreenBox"

export default function BacktestResult({ runBacktest }: any) {
    const selectedSymbol = useBacktestResultStore(s => s.selectedSymbol)
    const setSelectedSymbol = useBacktestResultStore(s => s.setSelectedSymbol)
    const statTab = selectedSymbol ?? "average"

    const equity = useBacktestResultStore(s => s.equity)
    const stats = useBacktestResultStore(s => s.stats)
    const trades = useBacktestResultStore(s => s.trades)
    const bestSharpe = stats?.average?.["Best Sharpe Column"]
    const bestReturn = stats?.average?.["Best Return Column"]

    // 状态：高度占比 (上下) 和 宽度占比 (左右)
    const [fullSection, setFullSection] = useState<string | null>(null);
    const [topHeight, setTopHeight] = useState(50)
    const [leftWidth, setLeftWidth] = useState(66) // 初始占比约 2/3

    const containerRef = useRef<HTMLDivElement>(null)
    const isDraggingVert = useRef(false) // 垂直拖拽标记
    const isDraggingHoriz = useRef(false) // 水平拖拽标记

    const replaceEmji = (s: string) => s.replace(/⭐|🚀|/g, "").trim()
    const formatDate = (ts: any) => {
        const d = new Date(ts); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`
    }
    const formatNumber = (v: number) => (v == null || !Number.isFinite(v)) ? "-" : v.toFixed(2)

    const filteredTrades = useMemo(() => {
        if (!selectedSymbol || selectedSymbol === "average")
            return trades
        return trades.filter(t => t.Column === selectedSymbol)
    }, [trades, selectedSymbol])

    const formatStatValue = (key: string, value: any) => {
        if (value == null || value === "" || !isFinite(value)) {
            return String(value ?? "-")
        }

        const k = key.toLowerCase()

        // ===== 百分比 =====
        if (k.includes("%") || k.includes("return") || k.includes("drawdown") || k.includes("rate")) {
            return `${value.toFixed(2)}%`
        }

        // ===== 比率（Sharpe / Sortino / Calmar / Omega）=====
        if (k.includes("ratio") || k.includes("sharpe") || k.includes("sortino") || k.includes("calmar") || k.includes("omega")) {
            return value.toFixed(2)
        }

        // ===== 金额类 =====
        if (
            k.includes("value") ||
            k.includes("cash") ||
            k.includes("pnl") ||
            k.includes("profit") ||
            k.includes("fees")
        ) {
            return value.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 4
            })
        }

        // ===== 次数 =====
        if (k.includes("trades") || k.includes("count")) {
            return Math.round(value)
        }

        // ===== 默认 =====
        return value.toFixed(2)
    }
    // const formatNumber = (v: number) => {
    //     if (v == null || !Number.isFinite(v)) return "-"
    //     return v.toFixed(2)
    // }

    // --- 拖拽逻辑 ---
    const handleVertMouseDown = () => {
        isDraggingVert.current = true
        document.body.style.cursor = "row-resize"
    }

    const handleHorizMouseDown = () => {
        isDraggingHoriz.current = true
        document.body.style.cursor = "col-resize"
    }

    const handleMouseMove = (e: React.MouseEvent) => {
        if (!containerRef.current) return
        const rect = containerRef.current.getBoundingClientRect()

        if (isDraggingVert.current) {
            const newHeight = ((e.clientY - rect.top) / rect.height) * 100
            setTopHeight(Math.max(20, Math.min(80, newHeight)))
        }

        if (isDraggingHoriz.current) {
            const newWidth = ((e.clientX - rect.left) / rect.width) * 100
            setLeftWidth(Math.max(30, Math.min(85, newWidth)))
        }
    }

    const handleMouseUp = () => {
        isDraggingVert.current = false
        isDraggingHoriz.current = false
        document.body.style.cursor = "default"
    }


    const statTabs = useMemo(() => {
        if (!stats) return ["average"]

        return [
            "average",
            ...Object.keys(stats.details || {}).sort()
        ]
    }, [stats])

    const currentStats = useMemo(() => {
        if (!stats) return {}

        if (statTab === "average") {
            return stats.average || {}
        }

        return stats.details?.[statTab] || {}
    }, [stats, statTab])

    // ==================
    // stats → rows（useMemo 防抖）
    // ==================
    const statRows = useMemo(() => {
        return Object.entries(currentStats).map(([k, v], index) => ({
            id: index,
            name: k,
            // value: typeof v === "number" ? v.toFixed(4) : String(v),
            value: typeof v === "number"
                ? formatStatValue(k, v)
                : String(v),
        }))
    }, [currentStats])

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
            align: "left",

            // ✅ 正确位置
            cellClassName: (params) => {
                if (params.row.name.includes("Return") || params.row.name.includes("Best Sharpe"))
                    return "highlight"
                if (params.row.name.includes("Drawdown"))
                    return "danger"
                return ""
            }
        }
    ], [])

    const buildSeries = (equity: any) => {
        const {
            times = [],
            average = [],
            details = {},
            best_sharpe,
            best_return,
            meta
        } = equity

        const zip = (arr?: number[]) =>
            (arr || []).map((v, i) => [times[i], v])

        const series: any[] = []

        // ===== Portfolio =====
        series.push({
            name: "Portfolio",
            type: "line",
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 3 },
            data: zip(average)
        })

        // ===== details =====
        Object.entries(details || {}).forEach(([symbol, arr]) => {
            const isBestSharpe = symbol === meta?.best_sharpe_column
            const isBestReturn = symbol === meta?.best_return_column

            series.push({
                name: isBestSharpe
                    ? `⭐ ${symbol}`
                    : isBestReturn
                        ? `🚀 ${symbol}`
                        : symbol,

                type: "line",
                smooth: true,
                showSymbol: false,

                lineStyle: {
                    width: isBestSharpe || isBestReturn ? 4 : 1,
                    type: isBestSharpe ? "dashed" : "solid",
                    shadowBlur: isBestSharpe ? 10 : 0,
                    shadowColor: isBestSharpe ? "#2e7d32" : undefined
                },

                itemStyle: {
                    color: isBestSharpe
                        ? "#2e7d32"
                        : isBestReturn
                            ? "#ed6c02"
                            : undefined
                },

                data: zip(arr as number[])
            })
        })

        return series
    }

    const baseSeriesStyle = {
        type: "line",
        smooth: true,
        showSymbol: false,

        emphasis: {
            focus: "series"
        },

        blur: {
            opacity: 0.15
        },
    }
    // ==================
    // chart option（避免重复创建）
    // ==================
    const chartOption = useMemo(() => {
        if (!equity?.times) return {}

        const series = buildSeries(equity)

        const hasDetails = Object.keys(equity.details || {}).length > 0

        return {
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "cross" },
                formatter: (params: any[]) => {
                    const timeStr = formatDate(params[0].axisValue)

                    let res = `${timeStr}<br/>`

                    params.forEach(p => {
                        res += `${p.marker} ${p.seriesName}: ${formatNumber(p.value[1])}<br/>`
                    })

                    return res
                },
                backgroundColor: "rgba(0, 0, 0, 0.35)",

                borderColor: "rgba(255, 255, 255, 0.15)",
                borderWidth: 1,

                textStyle: {
                    fontSize: 16,
                    color: "#fff"
                },

                extraCssText: `
                    backdrop-filter: blur(6px);
                    border-radius: 6px;
                    padding: 8px;
                    `
            },

            // 🔥 关键：布局彻底修好
            grid: {
                top: hasDetails ? 80 : 40,   // legend 空间
                bottom: 60,                  // zoom 空间
                left: 50,
                right: 20
            },

            legend: hasDetails
                ? {
                    top: 0,
                    type: "scroll",

                    selectedMode: "multiple",
                    inactiveColor: "#999",

                    textStyle: {
                        fontSize: 14,
                        color: "#f7e30b"
                    },

                    selected: {
                        [selectedSymbol || ""]: true
                    },
                    formatter: (name: string) => {
                        if (name === equity.meta?.best_sharpe_column) return `⭐ ${name}`
                        if (name === equity.meta?.best_return_column) return `🚀 ${name}`
                        return name
                    }
                }
                : { show: false },

            toolbox: {
                feature: {
                    dataZoom: { yAxisIndex: "none" },
                    restore: {},
                    saveAsImage: {}
                }
            },

            dataZoom: [
                { type: "inside" },
                {
                    type: "slider",
                    bottom: 10
                }
            ],

            xAxis: {
                type: "time"
            },

            yAxis: {
                type: "value",
                scale: true,
                axisLabel: {
                    formatter: (v: number) => v.toFixed(0)
                }
            },
            series: series.map(s => ({
                ...baseSeriesStyle,
                lineStyle: {
                    width:
                        selectedSymbol && s.name === selectedSymbol
                            ? 3
                            : 1,
                    opacity:
                        selectedSymbol && s.name !== selectedSymbol
                            ? 0.2
                            : 1
                },
                ...s
            }))
        }
    }, [equity])

    return (
        <Card
            ref={containerRef}
            sx={{
                p: 2,
                minHeight: 0,   // 🔥 必须加
                height: "100%",
                display: "flex",
                flexDirection: "column",
                boxSizing: "border-box"
            }}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            onMouseLeave={handleMouseUp}
        >
            {/* <Button variant="contained" onClick={runBacktest} sx={{ mb: 2 }}>
                Run Backtest
            </Button> */}

            {/* 上半 */}
            <Box sx={{
                display: "flex",
                height: `${topHeight}%`,
                minHeight: 0,
                width: "100%",
                flexShrink: 0
            }}>
                {/* 图表 */}
                <Box sx={{
                    width: `${leftWidth}%`,
                    minHeight: 0,
                    height: "100%",
                    overflow: "hidden"
                }}>
                    {/* Chart: 显式设置 width 百分比 */}
                    <FullScreenBox
                        isFull={fullSection === 'chart'}
                        onToggle={() => setFullSection(fullSection === 'chart' ? null : 'chart')}
                        sx={{ flex: 1, minHeight: 0, minWidth: 0, flexShrink: 0 }}
                    >
                        <ReactECharts
                            option={chartOption}
                            style={{ height: "100%", width: "100%" }}
                            notMerge={true}
                            opts={{ renderer: "canvas" }}
                            lazyUpdate={true}   // 🔥 防止频繁重算
                            onEvents={{
                                legendselectchanged: (params: any) => {
                                    const selected = Object.keys(params.selected)
                                        .find(k => params.selected[k] === true)

                                    if (selected && selected !== "Portfolio") {
                                        setSelectedSymbol(replaceEmji(selected))
                                    } else {
                                        setSelectedSymbol(null)
                                    }
                                },

                                click: (params: any) => {
                                    if (params.seriesName && params.seriesName !== "Portfolio") {
                                        setSelectedSymbol(replaceEmji(params.seriesName))
                                    } else {
                                        setSelectedSymbol(null)
                                    }
                                }
                            }}

                        />
                    </FullScreenBox>
                </Box>

                {/* 2. 垂直分隔条 (Splitter) */}
                <Box
                    onMouseDown={handleHorizMouseDown}
                    sx={{
                        width: 5,
                        mx: 0.5,
                        cursor: "col-resize",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        borderRadius: 1,
                        transition: 'background 0.2s',
                        "&:hover": { bgcolor: "primary.main", opacity: 0.5 },
                        "&::after": { // 中间的装饰线
                            content: '""',
                            width: 2,
                            height: 30,
                            bgcolor: "divider",
                            borderRadius: 1
                        }
                    }}
                />

                {/* 3. Stats 区 */}
                <FullScreenBox
                    isFull={fullSection === 'stats'}
                    onToggle={() => setFullSection(fullSection === 'stats' ? null : 'stats')}
                    sx={{ flex: 1, minHeight: 0, minWidth: 0 }} // minWidth: 0 允许它被压缩
                >
                    <Box sx={{
                        flex: 1, // 填充剩余宽度
                        // width: `${100 - leftWidth}%`,
                        height: "100%",
                        display: "flex",
                        flexDirection: "column",
                        minWidth: 0,
                        minHeight: 0
                    }}>
                        {/* Tabs */}
                        <Tabs
                            value={statTab}
                            onChange={(_, v) => {
                                if (v === "average") {
                                    setSelectedSymbol(null)
                                } else {
                                    setSelectedSymbol(replaceEmji(v))
                                }
                            }}
                            variant="scrollable"
                            scrollButtons="auto"
                            sx={{ minHeight: 32, minWidth: 0, width: "100%" }}
                        >
                            {statTabs.map(tab => {
                                const isSharpe = tab === bestSharpe
                                const isReturn = tab === bestReturn

                                return (
                                    <Tab
                                        key={tab}
                                        value={tab}
                                        label={
                                            tab === "average"
                                                ? "Portfolio"
                                                : isSharpe
                                                    ? `⭐ ${tab}`
                                                    : isReturn
                                                        ? `🚀 ${tab}`
                                                        : tab
                                        }
                                        sx={{
                                            minHeight: 32,

                                            // 🔥 核心高亮
                                            color: isSharpe
                                                ? "#2e7d32"
                                                : isReturn
                                                    ? "#ed6c02"
                                                    : undefined,

                                            fontWeight: isSharpe || isReturn ? 600 : 400
                                        }}
                                    />
                                )
                            })}
                        </Tabs>

                        {/* Table */}
                        <Box sx={{ flex: 1, minHeight: 0, minWidth: 0 }}>
                            <UniDataGrid
                                rows={statRows}
                                columns={statColumns}
                                autoHeight={false}
                                disableRowSelectionOnClick
                                hideFooterPagination
                                hideFooter
                                sx={{
                                    height: "100%",
                                    fontSize: "16px",

                                    "& .highlight": {
                                        color: "#4cd753",
                                        fontWeight: 600
                                    },

                                    "& .danger": {
                                        color: "#f03535",
                                        fontWeight: 600
                                    }, flex: 1, minHeight: 0, minWidth: 0
                                }}
                            />
                        </Box>

                    </Box>
                </FullScreenBox>
            </Box>

            {/* 水平分隔条 (上下拖拽) */}
            <Box
                onMouseDown={handleVertMouseDown}
                sx={{
                    height: 5,
                    cursor: "row-resize",
                    my: 1,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    "&:hover": { bgcolor: "primary.main", opacity: 0.5 },
                    "&::after": {
                        content: '""',
                        width: 40,
                        height: 2,
                        bgcolor: "divider",
                        borderRadius: 1
                    }
                }}
            />

            {/* 下半部分：Trades */}
            <FullScreenBox
                isFull={fullSection === 'trades'}
                onToggle={() => setFullSection(fullSection === 'trades' ? null : 'trades')}
                sx={{ height: "100%", flex: 1, minHeight: 0, minWidth: 0 }}
            >
                <Box sx={{
                        flex: 1, // 填充剩余宽度
                        height: "100%",
                        display: "flex",
                        flexDirection: "column",
                        minWidth: 0,
                        minHeight: 0
                    }}>
                    <TradesTable trades={filteredTrades} />
                </Box>
            </FullScreenBox>
        </Card>
    )
}