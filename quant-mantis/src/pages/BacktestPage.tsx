// pages/BacktestPage.tsx

import { Box } from "@mui/material"
import TopToolbar from "../components/layout/TopToolbar"
import PortfolioPanel from "../components/portfolio/PortfolioPanel"
import BacktestResult from "../components/backtest/BacktestResult"
import StrategyGraph from "../components/backtest/StrategyGraph"

export default function BacktestPage() {
  return (
    <Box sx={{ height: "100vh", display: "flex", flexDirection: "column" }}>

      {/* Top */}
      <TopToolbar />

      {/* Main */}
      <Box sx={{ flex: 1, display: "flex", minHeight: 0 }}>

        {/* LEFT: Portfolio Editor */}
        <Box
          sx={{
            width: 320,
            resize: "horizontal",
            overflow: "auto",
            borderRight: "1px solid #333",
            p: 1
          }}
        >
          <PortfolioPanel />
        </Box>

        {/* RIGHT: Result */}
        <Box
          sx={{
            flex: 1,
            display: "flex",
            flexDirection: "column",
            minWidth: 0
          }}
        >
          <BacktestResult />
        </Box>

      </Box>

      {/* Bottom */}
      <Box
        sx={{
          height: 260,
          borderTop: "1px solid #333",
          p: 1
        }}
      >
        <StrategyGraph />
      </Box>

    </Box>
  )
}