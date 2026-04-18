import { Stack, Button } from "@mui/material"
import StrategyCard from "./StrategyCard"
import { useBacktestStore } from "../../store/backtest.store"

export default function StrategyList() {
  const { strategies, addStrategy } = useBacktestStore()

  return (
    <Stack spacing={2}>
      {strategies.map((s, i) => (
        <StrategyCard key={s.id} index={i} strategy={s} />
      ))}

      <Button onClick={addStrategy}>
        + Add Strategy
      </Button>
    </Stack>
  )
}