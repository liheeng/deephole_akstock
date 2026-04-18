import UniDataGrid from "../table/UniDataGrid"
import { Card, Typography } from "@mui/material"

export default function TradesTable({ trades = [] }: any) {

  // 👇 完全按照你后端返回的真实字段定义
  const columns = [
    { field: "Symbol", headerName: "股票代码", flex: 1.5 },
    { field: "Entry Timestamp", headerName: "开仓时间", flex: 1.5 },
    { field: "Exit Timestamp", headerName: "平仓时间", flex: 1.5 },
    { field: "Column", headerName: "指标", flex: 1 },
    { field: "Direction", headerName: "方向", flex: 0.8 },
    { field: "Avg Entry Price", headerName: "开仓价", flex: 1 },
    { field: "Avg Exit Price", headerName: "平仓价", flex: 1 },
    { field: "Size", headerName: "数量", flex: 1 },
    { field: "PnL", headerName: "盈亏", flex: 1.2,
      renderCell: (params: any) => (
        <Typography color={params.value >= 0 ? "green" : "red"}>
          {params.value?.toFixed(2)}
        </Typography>
      )
    },
    { field: "Return", headerName: "收益率", flex: 1,
      renderCell: (params: any) => (
        <Typography color={params.value >= 0 ? "green" : "red"}>
          {(params.value * 100).toFixed(2)}%
        </Typography>
      )
    },
    { field: "Status", headerName: "状态", flex: 0.8 },
    { field: "Position Id", headerName: "持仓ID", flex: 0.8 },
    { field: "Exit Trade Id", headerName: "交易ID", flex: 0.8 },
  ]

  const rows = trades.map((t: any, i: number) => ({
    id: t["Exit Trade Id"] || i, // 用真实ID
    "Symbol": "", 
    ...t
  }))

  return (
    <Card sx={{ mt: 2 }}>
      <UniDataGrid
        rows={rows}
        columns={columns}
        autoHeight
        pageSizeOptions={[10, 20, 50]}
        disableRowSelectionOnClick
      />
    </Card>
  )
}