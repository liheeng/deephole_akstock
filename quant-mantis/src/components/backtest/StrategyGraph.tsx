import { Box, Typography } from "@mui/material"

export default function StrategyGraph() {
  return (
    <Box
      sx={{
        height: "100%",
        border: "1px dashed #555",
        display: "flex",
        alignItems: "center",
        justifyContent: "center"
      }}
    >
      <Typography color="text.secondary">
        Strategy Graph Area
      </Typography>
    </Box>
  )
}