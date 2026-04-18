// components/factor/FactorList.tsx
import { Box } from "@mui/material";
import FactorItem from "./FactorItem";
import { useBacktestStore } from "../../store/backtest.store";

export default function FactorList({ strategyIndex }: any) {
  // 精准订阅，只有当前策略的 factors 变化时才重新渲染列表
  const factors = useBacktestStore(s => s.strategies[strategyIndex]?.factors || []);

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
      {factors.map((f: any, i: number) => (
        <FactorItem
          key={f.id} // ✅ 保持 UUID 稳定
          strategyIndex={strategyIndex}
          factorIndex={i}
          factor={f}
          isLast={i === factors.length - 1} // 替代之前的逻辑判断
          canDelete={factors.length > 1}    // 至少保留一个因子
        />
      ))}
    </Box>
  );
}