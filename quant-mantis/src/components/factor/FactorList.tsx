// components/factor/FactorList.tsx

import { Box } from "@mui/material"
import { useMemo } from "react"

import FactorItem from "./FactorItem"
import { useStrategyStore } from "../../store/backtest/strategy.store"
export default function FactorList({ strategyId }: any) {

    // ✅ 只订阅当前 strategy 的 factorIds（极致精细）
    const factorIds = useStrategyStore(
        s => s.strategies[strategyId]?.factorIds
    )

    // ✅ 避免每次 render 生成新数组引用（可选优化
    const ids = useMemo(() => factorIds || [], [factorIds])

    return (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>

            {ids.map((factorId, i) => (
                <FactorItem
                    key={factorId}               // ✅ 稳定 key（核心）
                    strategyId={strategyId}
                    factorId={factorId}
                    isLast={i === ids.length - 1}
                    canDelete={ids.length > 1}
                />
            ))}

        </Box>
    )
}