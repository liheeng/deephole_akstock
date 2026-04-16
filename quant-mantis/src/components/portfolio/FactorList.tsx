// FactorList.tsx

import { useState } from "react"
import { Stack, Button } from "@mui/material"
import FactorRow from "./FactorRow"

export default function FactorList() {
  const [rows, setRows] = useState([""])

  return (
    <Stack spacing={1}>
      {rows.map((r, i) => (
        <FactorRow key={i} />
      ))}
      <Button onClick={() => setRows([...rows, ""])}>+ Factor</Button>
    </Stack>
  )
}