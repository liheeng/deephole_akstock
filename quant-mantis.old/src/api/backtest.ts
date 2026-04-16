import { api } from "./client"

export const runBacktest = (payload: any) =>
  api.post("/backtest", payload).then(r => r.data)

export const fetchNodes = () =>
  api.get("/nodes").then(r => r.data)