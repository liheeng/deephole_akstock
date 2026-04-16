import { DataGrid } from "@mui/x-data-grid"
import { Card } from "@mui/material"

export default function TradesTable({ trades = [] }: any) {

  const columns = [
    { field: "symbol", headerName: "Symbol", flex: 1 },
    { field: "entry", headerName: "Entry", flex: 1 },
    { field: "exit", headerName: "Exit", flex: 1 },
    { field: "pnl", headerName: "PnL", flex: 1 }
  ]

  const rows = trades.map((t: any, i: number) => ({
    id: i,
    ...t
  }))

  return (
    <Card sx={{ mt: 2 }}>
      <DataGrid
        rows={rows}
        columns={columns}
        autoHeight
        pageSizeOptions={[10, 20, 50]}
      />
    </Card>
  )
}