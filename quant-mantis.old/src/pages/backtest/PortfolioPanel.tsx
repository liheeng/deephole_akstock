import { Card, TextField, Stack, Switch } from "@mui/material"

export function PortfolioPanel() {
  return (
    <Card sx={{ p: 2, mt: 2 }}>

      <Stack spacing={2}>

        <TextField label="init_cash" size="small" />

        <TextField label="top_n" size="small" />

        <TextField label="hold_days" size="small" />

        <TextField label="freq" size="small" />

        <Switch defaultChecked /> Schedule Signal

      </Stack>

    </Card>
  )
}