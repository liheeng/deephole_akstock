// components/portfolio/StrategyList.tsx

import { useState } from "react"
import { Stack, Button } from "@mui/material"
import StrategyCard from "./StrategyCard"

export default function StrategyList({ mode }: any) {
  const [strategies, setStrategies] = useState([{}])

  return (
    <Stack spacing={2}>
      {strategies.map((s, i) => (
        <StrategyCard key={i} mode={mode} />
      ))}

      <Button onClick={() => setStrategies([...strategies, {}])}>
        + Add Strategy
      </Button>
    </Stack>
  )
}