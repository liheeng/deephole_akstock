import { Box, Tabs, Tab, Switch, FormControlLabel, Select, MenuItem, Typography } from "@mui/material";
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import UniDataGrid from "../table/UniDataGrid";
import { FullScreenBox } from "../misc/FullScreenBox";
import { useMemo, useEffect } from "react";

export const StatsPanel = ({ fullSection, setFullSection, viewMode, setViewMode }: any) => {
    const { stats, selectedSymbol, setSelectedSymbol } = useBacktestResultStore();

    const statTabs = useMemo(() => ["average", ...Object.keys(stats?.details || {}).sort()], [stats]);
    
    const currentStats = useMemo(() => {
        if (viewMode === "portfolio") return stats?.average || {};
        return stats?.details?.[selectedSymbol || ""] || {};
    }, [stats, selectedSymbol, viewMode]);

    const rows = Object.entries(currentStats).map(([k, v], i) => ({
        id: i, name: k, value: typeof v === "number" ? v.toFixed(2) : String(v)
    }));

    // 在 StatsPanel.tsx 内部增加一个 useEffect
    useEffect(() => {
        if (viewMode === "individual" && !selectedSymbol) {
            const firstSymbol = Object.keys(stats?.details || {})[0];
            if (firstSymbol) setSelectedSymbol(firstSymbol);
        }
    }, [viewMode, stats, selectedSymbol]);

    return (
        <FullScreenBox
            isFull={fullSection === 'stats'}
            onToggle={() => setFullSection(fullSection === 'stats' ? null : 'stats')}
            sx={{ display: "flex", flexDirection: "column", height: "100%" }}
        >
            <Box sx={{ p: 1, display: "flex", alignItems: "center", gap: 2, borderBottom: 1, borderColor: 'divider' }}>
                <FormControlLabel
                    control={<Switch size="small" checked={viewMode === "individual"} onChange={(e) => setViewMode(e.target.checked ? "individual" : "portfolio")} />}
                    label={<Typography variant="caption">Individual Mode</Typography>}
                />
                {viewMode === "individual" && (
                    <Select
                        size="small"
                        value={selectedSymbol || ""}
                        onChange={(e) => setSelectedSymbol(e.target.value)}
                        sx={{ height: 30, fontSize: '0.8rem', minWidth: 120 }}
                    >
                        {Object.keys(stats?.details || {}).map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
                    </Select>
                )}
            </Box>

            {viewMode === "portfolio" && (
                <Tabs 
                    value={selectedSymbol ?? "average"} 
                    onChange={(_, v) => setSelectedSymbol(v === "average" ? null : v)} 
                    variant="scrollable"
                    sx={{ minHeight: 40 }}
                >
                    {statTabs.map(t => <Tab key={t} value={t} label={t} sx={{ py: 0.5, minHeight: 40 }} />)}
                </Tabs>
            )}

            <Box sx={{ flex: 1, minHeight: 0 }}>
                <UniDataGrid
                    rows={rows}
                    columns={[{ field: "name", headerName: "Indicator", flex: 1 }, { field: "value", headerName: "Value", flex: 1 }]}
                    hideFooter
                />
            </Box>
        </FullScreenBox>
    );
};