// components/factor/FactorItem.tsx
import React, { useCallback } from "react";
import { Box, IconButton } from "@mui/material";
import DSLInput from "../dsl/DSLInput";
import SettingsIcon from "@mui/icons-material/Settings";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import { useBacktestStore } from "../../store/backtest.store";

const FactorItem = React.memo(({
    strategyIndex,
    factorIndex,
    factor,
    isLast,
    canDelete
}: any) => {
    // ✅ 直接从 store 中提取稳定的 Action 引用
    const updateFactor = useBacktestStore(s => s.updateFactor);
    const addFactor = useBacktestStore(s => s.addFactor);
    const deleteFactor = useBacktestStore(s => s.deleteFactor);
    const openDialog = useBacktestStore(s => s.openDialog);

    // ✅ 这里的 handleChange 引用现在是永久稳定的
    const handleChange = useCallback((v: string) => {
        updateFactor(strategyIndex, factorIndex, v);
    }, [strategyIndex, factorIndex, updateFactor]);

    // ✅ 按钮逻辑也改为内部处理，避免父组件传递匿名函数
    const handleAdd = useCallback(() => {
        addFactor(strategyIndex, factorIndex);
    }, [strategyIndex, factorIndex, addFactor]);

    const handleDelete = useCallback(() => {
        deleteFactor(strategyIndex, factorIndex);
    }, [strategyIndex, factorIndex, deleteFactor]);

    const handleOpenSettings = useCallback(() => {
        openDialog({
            type: "factor",
            strategyIndex,
            factorIndex
        });
    }, [strategyIndex, factorIndex, openDialog]);

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
    );
});

export default FactorItem;