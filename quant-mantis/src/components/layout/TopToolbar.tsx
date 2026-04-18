// components/layout/TopToolbar.tsx

import { Box, Button } from "@mui/material"

export default function TopToolbar({runBacktest}: any) {
  return (
    <Box
      sx={{
        height: 56,
        display: "flex",
        alignItems: "center",
        px: 2,
        borderBottom: "1px solid #333"
      }}
    >
      <Button variant="contained" onClick={runBacktest}>Run</Button>
      <Button sx={{ ml: 1 }}>Save</Button>
    </Box>
  )
}