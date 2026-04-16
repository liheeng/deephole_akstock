import { useState } from "react"
import { FactorRow } from "./FactorRow"
import { Button, Stack } from "@mui/material"

export type Factor = {
  expr: string
  lockedAdd: boolean
  canDelete: boolean
}

export function FactorList() {
  const [factors, setFactors] = useState<Factor[]>([
    { expr: "", lockedAdd: false, canDelete: false }
  ])

  const update = (i: number, f: Factor) => {
    const copy = [...factors]
    copy[i] = f
    setFactors(copy)
  }

  const addNext = (i: number) => {
    const copy = [...factors]

    copy[i].lockedAdd = true
    copy[i].canDelete = true

    copy.push({
      expr: "",
      lockedAdd: false,
      canDelete: false
    })

    setFactors(copy)
  }

  const remove = (i: number) => {
    setFactors(factors.filter((_, idx) => idx !== i))
  }

  return (
    <Stack spacing={1}>
      {factors.map((f, i) => (
        <FactorRow
          key={i}
          factor={f}
          index={i}
          onChange={update}
          onAddNext={addNext}
          onDelete={remove}
        />
      ))}
    </Stack>
  )
}