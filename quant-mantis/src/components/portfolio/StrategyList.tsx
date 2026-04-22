import { Stack, Button } from "@mui/material"
import StrategyCard from "./StrategyCard"
import { useStrategyStore } from "../../store/backtest/strategy.store"
import { useBacktestStore } from "../../store/backtest/backtest.store"

export default function StrategyList() {

    // ✅ 只订阅 ID 列表（关键优化点）
    const strategyIds = useStrategyStore(s => s.strategyIds)

    const addStrategy = useStrategyStore(s => s.createStrategy)
    const portfolioMode = useBacktestStore(s => s.portfolio_mode)

    return (
        <Stack spacing={2}>
            {strategyIds.map((id) => (
                <StrategyCard
                    key={id}          // ✅ 稳定 key
                    strategyId={id}   // ✅ 不再用 index
                />
            ))}

            <Button onClick={() => addStrategy(portfolioMode === "signal_strategy" ? "ts" : "cs")}>
                + Add Strategy
            </Button>
        </Stack>
    )
}