import { Card, TextField } from "@mui/material"

export function StrategyPanel() {
  return (
    <Card sx={{ p: 2 }}>
      <TextField fullWidth label="Strategy Name" size="small" />
    </Card>
  )
}