// components/factor/FactorItem.tsx

import { Box, IconButton } from "@mui/material"
import DSLInput from "../dsl/DSLInput"
import SettingsIcon from "@mui/icons-material/Settings"
import AddIcon from "@mui/icons-material/Add"
import DeleteIcon from "@mui/icons-material/Delete"

import { useBacktestStore } from "../../store/backtest.store"
// import { useNodes } from "../../hooks/useNodes"

export default function FactorItem({
    strategyIndex,
    factors,
    factorIndex,
    factor,
    onChange,
    onAdd,
    onDelete,
    canDelete
}: any) {

    const openDialog = useBacktestStore(s => s.openDialog)
    // const nodes = useNodes()

    return (
        <Box sx={{display:"flex", gap:1, alignItems:"center"}}>

            {/* DSL */}
            <Box sx={{flex:1}}>
                <DSLInput
                    value={factor.expr}
                    onChange={(v: any) => onChange(v)}
                />
            </Box>

            {/* Visual Editor */}
            <IconButton
                onClick={() =>
                    openDialog({
                        type: "factor",
                        strategyIndex,
                        factorIndex
                    })
                }
            >
                <SettingsIcon />
            </IconButton>

            {/* Add */}
            {/* <IconButton onClick={onAdd} disabled={!factor.added || !factor.expr}> */}
            <IconButton onClick={onAdd} disabled={factorIndex != (factors.length - 1)}>
                <AddIcon />
            </IconButton>

            {/* Delete */}
            <IconButton onClick={onDelete} disabled={!canDelete}>
                <DeleteIcon />
            </IconButton>

        </Box>
    )
}