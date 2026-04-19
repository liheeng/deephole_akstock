import { Stack, Button } from "@mui/material"
import StrategyCard from "./StrategyCard"
import { useStrategyStore } from "../../store/backtest/strategy.store"
export default function StrategyList() {

    // ✅ 只订阅 ID 列表（关键优化点）
    const strategyIds = useStrategyStore(s => s.strategyIds)

    const addStrategy = useStrategyStore(s => s.createStrategy)
   
    return (
        <Stack spacing={2}>
            {strategyIds.map((id) => (
                <StrategyCard
                    key={id}          // ✅ 稳定 key
                    strategyId={id}   // ✅ 不再用 index
                />
            ))}

            <Button onClick={addStrategy}>
                + Add Strategy
            </Button>
        </Stack>
    )
}