import { api } from "./client"

export const runBacktest = async (payload: any) => {
  const res = await api.post("/backtest", payload)
  return res.data
}