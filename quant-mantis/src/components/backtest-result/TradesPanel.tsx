import TradesTable from "../backtest/TradesTable";
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import { useMemo } from "react";
import { FullScreenBox } from "../misc/FullScreenBox";

export default function TradesPanel({ fullSection, setFullSection }: any) {
  const { trades, selectedSymbol } = useBacktestResultStore();

  const filteredTrades = useMemo(() => {
    if (!selectedSymbol || selectedSymbol === "average") return trades;
    return trades.filter(t => t.Column === selectedSymbol);
  }, [trades, selectedSymbol]);

  return (
    <FullScreenBox
      isFull={fullSection === "trades"}
      onToggle={() => setFullSection(fullSection === "trades" ? null : "trades")}
      sx={{ height: "100%", flex: 1 }}
    >
       <TradesTable trades={filteredTrades} />
    </FullScreenBox>
  );
}