import { Card, Typography } from "@mui/material"
import ReactECharts from "echarts-for-react"

export function BacktestResult() {
  const option = {
    xAxis: { type: "category", data: [] },
    yAxis: { type: "value" },
    series: [{ type: "line", data: [] }]
  }

  return (
    <Card sx={{ p: 2 }}>
      <Typography variant="h6">Equity Curve</Typography>
      <ReactECharts option={option} />
    </Card>
  )
}