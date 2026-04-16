// components/portfolio/PortfolioPanel.tsx

import { useState } from "react"
import { Stack, Select, MenuItem, TextField } from "@mui/material"
import StrategyList from "./StrategyList"

export default function PortfolioPanel() {
  const [mode, setMode] = useState("signal")

  return (
    <Stack spacing={2}>

      <Select value={mode} onChange={(e) => setMode(e.target.value)} size="small">
        <MenuItem value="signal">Signal</MenuItem>
        <MenuItem value="weight">Weight</MenuItem>
      </Select>

      <TextField label="Schedule Signal" size="small" />

      <StrategyList mode={mode} />

      {mode === "signal" ? (
        <>
          <TextField label="Strategy Op (AND/OR)" size="small" />
          <TextField label="Vote Weights" size="small" />
        </>
      ) : (
        <TextField label="Strategy Weights" size="small" />
      )}

      <TextField label="freq" size="small" defaultValue="1D" />
      <TextField label="init_cash" size="small" defaultValue="100000" />

    </Stack>
  )
}