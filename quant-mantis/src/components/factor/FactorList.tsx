// components/factor/FactorList.tsx

import { Box } from "@mui/material"
import FactorItem from "./FactorItem"
import { useBacktestStore } from "../../store/backtest.store"

export default function FactorList({ strategyIndex }: any) {

  const {
    strategies,
    updateFactor,
    addFactor,
    deleteFactor
  } = useBacktestStore()

  const factors = strategies[strategyIndex]?.factors || []

  return (
    <Box sx={{display: "flex", flexDirection:"column", gap:1}}>

      {factors.map((f: any, i: number) => (
        <FactorItem
          key={i}
          strategyIndex={strategyIndex}
          factorIndex={i}
          factor={f}
          onChange={(v: string) => updateFactor(strategyIndex, i, v)}
          onAdd={() => addFactor(strategyIndex, i)}
          onDelete={() => deleteFactor(strategyIndex, i)}
          canDelete={i > 0}
        />
      ))}

    </Box>
  )
}