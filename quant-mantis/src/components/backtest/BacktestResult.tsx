import { Card, Button, Box, Grid, Typography, Paper } from "@mui/material"
import ReactECharts from "echarts-for-react"
import TradesTable from "./TradesTable"
import { useBacktestStore } from "../../store/backtest.store"

export default function BacktestResult({ runBacktest }: any) {

    const backtestResult = useBacktestStore(state => state.backtestResult)

    const equity = backtestResult?.equity || {}
    const stats = backtestResult?.stats || {}

    const dates = Object.keys(equity)
    const values = Object.values(equity)

    return (
        <Card
            sx={{
                p: 2,
                height: "100%",
                display: "flex",
                flexDirection: "column"
            }}
        >

            <Button variant="contained" onClick={runBacktest}>
                Run Backtest
            </Button>

            {/* ===== Equity Chart ===== */}
            <Box sx={{ flex: 1, minHeight: 300, mt: 2 }}>
                <ReactECharts
                    option={{
                        tooltip: { trigger: "axis" },
                        xAxis: { type: "category", data: dates },
                        yAxis: { type: "value", scale: true },
                        series: [
                            {
                                name: "Equity",
                                type: "line",
                                smooth: true,
                                showSymbol: false,
                                data: values
                            }
                        ]
                    }}
                    style={{ height: "100%" }}
                />
            </Box>

            {/* ===== Stats ===== */}
            <Box sx={{ mt: 2 }}>
                <Typography variant="h6">Stats</Typography>

                <Grid container spacing={2}>
                    {Object.entries(stats).map(([key, value]) => (
                        <Grid item xs={3} key={key}>
                            <Paper sx={{ p: 2 }}>
                                <Typography variant="caption">{key}</Typography>
                                <Typography variant="h6">
                                    {typeof value === "number"
                                        ? value.toFixed(4)
                                        : String(value)}
                                </Typography>
                            </Paper>
                        </Grid>
                    ))}
                </Grid>
            </Box>

            {/* ===== Trades ===== */}
            <Box sx={{ height: 200, mt: 2 }}>
                <TradesTable trades={backtestResult?.trades || []} />
            </Box>

        </Card>
    )
}