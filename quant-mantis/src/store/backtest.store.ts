import { create } from "zustand"

export const useBacktestStore = create((set) => ({
  factors: [
    { expr: "", lockedAdd: false, canDelete: false }
  ],
  strategies: [{ name: "strategy_1", signal: "" }],
  result: null,

  setFactors: (f: any) => set({ factors: f }),
  setResult: (r: any) => set({ result: r })
}))