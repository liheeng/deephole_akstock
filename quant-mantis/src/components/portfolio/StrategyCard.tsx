// components/portfolio/StrategyCard.tsx

import { Card, TextField } from "@mui/material"
import FactorList from "./FactorList"

export default function StrategyCard({ mode }: any) {
  return (
    <Card sx={{ p: 1 }}>

      <TextField fullWidth size="small" label="Strategy Name" />

      <FactorList />

      <TextField fullWidth size="small" label="Signal DSL" />

      {mode === "signal" && (
        <TextField fullWidth size="small" label="Threshold" />
      )}

      {mode === "weight" && (
        <TextField fullWidth size="small" label="Top N" />
      )}

    </Card>
  )
}