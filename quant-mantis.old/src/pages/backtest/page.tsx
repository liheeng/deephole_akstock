import { Grid, Container } from "@mui/material"
import { StrategyPanel } from "../../components/backtest/StrategyPanel"
import { FactorList } from "../../components/backtest/FactorList"
import { PortfolioPanel } from "../../components/backtest/PortfolioPanel"
import { BacktestResult } from "../../components/backtest/BacktestResult"

export default function BacktestPage() {
  return (
    <Container maxWidth={false}>

      <Grid container spacing={2}>

        {/* LEFT */}
        <Grid item xs={3}>
          <StrategyPanel />
          <PortfolioPanel />
        </Grid>

        {/* CENTER */}
        <Grid item xs={6}>
          <BacktestResult />
        </Grid>

        {/* RIGHT */}
        <Grid item xs={3}>
          <FactorList />
        </Grid>

      </Grid>

    </Container>
  )
}