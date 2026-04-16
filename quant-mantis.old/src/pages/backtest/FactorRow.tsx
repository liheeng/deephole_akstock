import { TextField, Button, Stack, Card } from "@mui/material"

export function FactorRow({
  factor,
  index,
  onChange,
  onAddNext,
  onDelete
}) {
  return (
    <Card sx={{ p: 1 }}>
      <Stack direction="row" spacing={1}>

        {/* DSL */}
        <TextField
          size="small"
          fullWidth
          value={factor.expr}
          onChange={(e) =>
            onChange(index, { ...factor, expr: e.target.value })
          }
        />

        {/* Visual */}
        <Button size="small" variant="outlined">
          Visual
        </Button>

        {/* Add */}
        <Button
          size="small"
          variant="contained"
          disabled={factor.lockedAdd}
          onClick={() => onAddNext(index)}
        >
          Add
        </Button>

        {/* Delete */}
        <Button
          size="small"
          color="error"
          variant="outlined"
          disabled={!factor.canDelete}
          onClick={() => onDelete(index)}
        >
          Del
        </Button>

      </Stack>
    </Card>
  )
}