import { Card, Button, Box } from "@mui/material"
import ReactECharts from "echarts-for-react"
import TradesTable from "./TradesTable"

export default function BacktestResult() {

  return (
    <Card 
        sx={{
            p: 2,
            height: "100%",
            display: "flex",
            flexDirection: "column"
        }}>

      <Button variant="contained">Run Backtest</Button>

      <Box sx={{ flex: 1, minHeight: 200 }}>
        <ReactECharts
          option={{
            xAxis: { type: "category", data: [] },
            yAxis: { type: "value" },
            series: [{ type: "line", data: [] }]
          }}
          style={{ height: "100%" }}
        />
      </Box>

      <Box sx={{ height: 200 }}>
        <TradesTable />
      </Box>

    </Card>
  )
}