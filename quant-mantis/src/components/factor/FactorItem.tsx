// components/factor/FactorItem.tsx

import React, { useCallback } from "react"
import { Box, IconButton } from "@mui/material"
import DSLInput from "../dsl/DSLInput"

import SettingsIcon from "@mui/icons-material/Settings"
import AddIcon from "@mui/icons-material/Add"
import DeleteIcon from "@mui/icons-material/Delete"

import { useFactorStore } from "../../store/backtest/factor.store"
import { useStrategyStore } from "../../store/backtest/strategy.store"
import { useDialogStore } from "../../store/uiDialog.store"

const FactorItem = React.memo(({
    strategyId,
    factorId,
    isLast,
    canDelete
}: any) => {

    // ===== state =====
    const factor = useFactorStore(s => s.factors[factorId])

    // ===== actions =====
    const updateFactor = useFactorStore(s => s.updateFactor)
    const createFactor = useFactorStore(s => s.createFactor)

    const addFactorToStrategy = useStrategyStore(s => s.addFactorToStrategy)
    const removeFactorFromStrategy = useStrategyStore(s => s.removeFactorFromStrategy)

    const openDialog = useDialogStore(s => s.openDialog)

    // =========================
    // handlers
    // =========================

    const handleChange = useCallback((v: string) => {
        updateFactor(factorId, v)
    }, [factorId, updateFactor])

    const handleAdd = useCallback(() => {
        const newId = createFactor()
        addFactorToStrategy(strategyId, newId)
    }, [strategyId, createFactor, addFactorToStrategy])

    const handleDelete = useCallback(() => {
        removeFactorFromStrategy(strategyId, factorId)
    }, [strategyId, factorId, removeFactorFromStrategy])

    const handleOpenSettings = useCallback(() => {
        openDialog("factor", factor)
    }, [factorId, openDialog])

    // =========================
    // render
    // =========================

    if (!factor) return null

    return (
        <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>

            <Box sx={{ flex: 1 }}>
                <DSLInput
                    value={factor.expr}
                    onChange={handleChange}
                />
            </Box>

            <IconButton onClick={handleOpenSettings}>
                <SettingsIcon />
            </IconButton>

            <IconButton onClick={handleAdd} disabled={!isLast}>
                <AddIcon />
            </IconButton>

            <IconButton onClick={handleDelete} disabled={!canDelete}>
                <DeleteIcon />
            </IconButton>

        </Box>
    )
})

export default FactorItem