// components/layout/TopToolbar.tsx
import { Box, Button } from "@mui/material"

export default function TopToolbar({ runBacktest, launchBacktestWizard }: any) {
  return (
    <Box
      sx={{
        height: 56,
        display: "flex",
        alignItems: "center",
        px: 2,
        borderBottom: "1px solid #333",
        gap: 2, // 👈 关键：自动给所有按钮加间距
      }}
    >
      <Button variant="contained" onClick={runBacktest}>
        🚀 Run
      </Button>
      
      <Button variant="contained" onClick={launchBacktestWizard}>
        🧪 Run with Wizard
      </Button>
      
      <Button variant="outlined">
        💾 Save
      </Button>
    </Box>
  )
}