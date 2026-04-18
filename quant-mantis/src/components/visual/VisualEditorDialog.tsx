import {
  Dialog,
  Box,
  Select,
  MenuItem,
  TextField,
  Button,
  Stack,
  Typography
} from "@mui/material"

import { useState, useRef } from "react"
import DSLInput from "../dsl/DSLInput"

export default function VisualEditorDialog({
  open,
  nodes,
  onClose,
  onConfirm
}: any) {

  const [expr, setExpr] = useState("")
  const [group, setGroup] = useState("indicator")
  const [selected, setSelected] = useState<any>(null)
  const [setParams] = useState<any>({})

  const editorRef = useRef<any>(null)

  // ===== 插入函数 =====
  const insertFunction = (node: any) => {
    setSelected(node)

    const snippet = `${node.name}(${node.params
      .map((p: any) => p.default ?? "")
      .join(", ")})`

    insertAtCursor(snippet)
  }

  const insertAtCursor = (text: string) => {
    const editor = editorRef.current
    if (!editor) return

    const selection = editor.getSelection()

    editor.executeEdits("", [
      {
        range: selection,
        text
      }
    ])
  }

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>

      <Box sx={{ p: 2 }}>

        <Typography variant="h6">Visual Editor</Typography>

        {/* ================= Expression ================= */}
        <Box sx={{ mt: 2 }}>
          <DSLInput
            value={expr}
            onChange={(v: string) => setExpr(v)}
          />
        </Box>

        {/* ================= Insert Panel ================= */}
        <Stack direction="row" spacing={1} sx={{ mt: 2 }}>

          {/* group */}
          <Select
            value={group}
            onChange={(e) => setGroup(e.target.value)}
            sx={{ width: 140 }}
          >
            {Object.keys(nodes).map(g => (
              <MenuItem key={g} value={g}>{g}</MenuItem>
            ))}
          </Select>

          {/* function list */}
          <Select
            value={selected?.name || ""}
            onChange={(e) => {
              const node = nodes[group].find((n: any) => n.name === e.target.value)
              insertFunction(node)
            }}
            sx={{ width: 200 }}   // ⭐和前面一样宽
          >
            {nodes[group]?.map((n: any) => (
              <MenuItem key={n.name} value={n.name}>
                {n.name}
              </MenuItem>
            ))}
          </Select>

        </Stack>

        {/* ================= Params ================= */}
        {selected && (
          <Stack direction="row" spacing={1} sx={{ mt: 2 }}>

            {selected.params.map((p: any) => (
              <TextField
                key={p.name}
                label={p.name}
                size="small"
                sx={{ width: 120 }}
                onChange={(e) => {
                  const v = e.target.value
                //   setParams(prev => ({ ...prev, [p.name]: v }))
                  setParams((prev: any) => ({ ...prev, [p.name]: v }));
                }}
              />
            ))}

          </Stack>
        )}

        {/* ================= Buttons ================= */}
        <Stack
          sx={{
            direction:"row",
            justifyContent:"flex-end",
            spacing:1,
            mt: 3
        }}
        >

          <Button onClick={onClose}>
            Cancel
          </Button>

          <Button
            variant="contained"
            onClick={() => onConfirm(expr)}
          >
            Confirm
          </Button>

        </Stack>

      </Box>
    </Dialog>
  )
}