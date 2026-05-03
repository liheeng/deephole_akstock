import { useState } from "react";
import {
    Box,
    Typography,
    TextField,
    Button,
    Checkbox,
    FormControlLabel,
    FormGroup,
    RadioGroup,
    Radio,
    Alert,
    CircularProgress,
    Paper,
    LinearProgress
} from "@mui/material";

import { apiClient } from "../../api/Client";
import UniDataGrid from "../../components/table/UniDataGrid";

const ALL_COLS = [
    "symbol", "symbol_name", "market", "date",
    "open", "high", "low", "close", "volume", "amount",
    "pct", "turnover", "adjust_mode", "adjust_factor"
];

export default function ExportDataPage() {
    const [selectedCols, setSelectedCols] = useState<string[]>(ALL_COLS);

    const [whereSql, setWhereSql] = useState("");
    const [groupBy, setGroupBy] = useState("");
    const [orderBy, setOrderBy] = useState("");
    const [limit, setLimit] = useState("");

    const [format, setFormat] = useState<"csv" | "parquet">("csv");

    const [loading, setLoading] = useState(false);
    const [progress, setProgress] = useState(0);

    const [error, setError] = useState<string | null>(null);
    const [successMsg, setSuccessMsg] = useState<string | null>(null);

    const [previewRows, setPreviewRows] = useState<any[]>([]);
    const [total, setTotal] = useState<number | null>(null);

    // ========================
    // 字段选择
    // ========================
    const handleCheck = (col: string) => {
        setSelectedCols((prev) =>
            prev.includes(col)
                ? prev.filter((c) => c !== col)
                : [...prev, col]
        );
    };

    // ========================
    // 预览
    // ========================
    const handlePreview = async () => {
        setError(null);

        if (selectedCols.length === 0) {
            setError("请至少选择一个字段");
            return;
        }

        try {
            const res = await apiClient.post(
                `/export/preview`,
                {
                    columns: selectedCols,
                    where: whereSql,
                    group_by: groupBy,
                    order_by: orderBy,
                    limit: parseInt(limit) > 50 ? '50' : parseInt(limit)
                },
                { withCredentials: true }
            );

            setPreviewRows(res.data.rows);
            setTotal(res.data.total);

        } catch (err: any) {
            setError(err?.response?.data || err.message);
        }
    };

    // ========================
    // 导出
    // ========================
    const handleExport = async () => {
        setError(null);
        setSuccessMsg(null);

        if (selectedCols.length === 0) {
            setError("请至少选择一个字段");
            return;
        }

        setLoading(true);
        setProgress(0);

        try {
            const res = await apiClient.post(
                `/export/stream`,
                {
                    columns: selectedCols,
                    where: whereSql,
                    group_by: groupBy,
                    order_by: orderBy,
                    limit: limit,
                    export_format: format
                },
                {
                    withCredentials: true,
                    responseType: "blob",
                    onDownloadProgress: (e) => {
                        if (e.total) {
                            const percent = Math.round((e.loaded * 100) / e.total);
                            setProgress(percent);
                        }
                    }
                }
            );

            // 下载
            const blob = new Blob([res.data]);
            const url = window.URL.createObjectURL(blob);

            const a = document.createElement("a");
            a.href = url;
            a.download = `stock_daily.${format}`;
            a.click();

            window.URL.revokeObjectURL(url);

            setSuccessMsg("导出成功");

        } catch (err: any) {
            setError(err?.response?.data || err.message);
        } finally {
            setLoading(false);
        }
    };

    // ========================
    // UI
    // ========================
    return (
        <Box
            sx={{
                p:3,
                height: "100vh",
                display: "flex",
                flexDirection: "column",
            }}
            >
            <Typography variant="h5" gutterBottom>
                📤 数据导出
            </Typography>

            {/* 字段选择 */}
            <Typography variant="subtitle1">选择字段</Typography>
            <Paper sx={{ p: 2, mb: 2 }}>
                <FormGroup row>
                    {ALL_COLS.map((col) => (
                        <FormControlLabel
                            key={col}
                            control={
                                <Checkbox
                                    checked={selectedCols.includes(col)}
                                    onChange={() => handleCheck(col)}
                                />
                            }
                            label={col}
                        />
                    ))}
                </FormGroup>
            </Paper>

            {/* 查询条件 */}
            <TextField
                fullWidth
                label="WHERE 条件"
                placeholder="market='CN' AND date >= '2025-01-01'"
                value={whereSql}
                onChange={(e) => setWhereSql(e.target.value)}
                sx={{ mb: 2 }}
            />

            <TextField
                fullWidth
                label="GROUP BY"
                value={groupBy}
                onChange={(e) => setGroupBy(e.target.value)}
                sx={{ mb: 2 }}
            />

            <TextField
                fullWidth
                label="ORDER BY"
                placeholder="date DESC"
                value={orderBy}
                onChange={(e) => setOrderBy(e.target.value)}
                sx={{ mb: 2 }}
            />

            <TextField
                fullWidth
                label="LIMIT"
                placeholder="不填表示全量（谨慎）"
                value={limit}
                onChange={(e) => setLimit(e.target.value)}
                sx={{ mb: 2 }}
            />

            {/* 格式 */}
            <RadioGroup
                row
                value={format}
                onChange={(e) => setFormat(e.target.value as any)}
                sx={{ mb: 2 }}
            >
                <FormControlLabel value="csv" control={<Radio />} label="CSV（推荐大文件）" />
                <FormControlLabel value="parquet" control={<Radio />} label="Parquet（小数据）" />
            </RadioGroup>

            {/* parquet提示 */}
            {format === "parquet" && (
                <Alert severity="warning" sx={{ mb: 2 }}>
                    ⚠️ Parquet 不适合超大数据，会占用大量内存
                </Alert>
            )}

            {/* 按钮 */}
            <Box sx={{mb:2}} >
                <Button variant="outlined" onClick={handlePreview} sx={{ mr: 2 }}>
                    👁 预览50条
                </Button>

                <Button
                    variant="contained"
                    onClick={handleExport}
                    disabled={loading}
                    startIcon={loading && <CircularProgress size={16} />}
                >
                    🚀 导出
                </Button>
            </Box>

            {/* 进度 */}
            {loading && (
                <Box sx={{mb:2}} >
                    <LinearProgress variant="determinate" value={progress} />
                    <Typography variant="body2">{progress}%</Typography>
                </Box>
            )}

            {/* 总数 */}
            {total !== null && (
                <Alert severity="info" sx={{ mb: 2 }}>
                    🔢 总记录数：{total.toLocaleString()}
                </Alert>
            )}

            {/* 错误 */}
            {error && (
                <Alert severity="error" sx={{ mb: 2 }}>
                    ❌ {error}
                </Alert>
            )}

            {/* 成功 */}
            {successMsg && (
                <Alert severity="success" sx={{ mb: 2 }}>
                    ✅ {successMsg}
                </Alert>
            )}

            {/* 预览 */}
            {previewRows.length > 0 && (
                <Paper
                    sx={{
                        p: 2,
                        flex: 1,
                        minHeight: 0,
                        display: "flex",
                        flexDirection: "column",
                    }}
                >
                    {/* ########## 滚动容器 ########## */}
                    <Box
                        sx={{
                            overflow: "auto", // 核心：开启滚动
                            flex: 1,
                            width: "100%",
                        }}
                    >
                        {/* ########## 强制给表格一个宽度，强行撑开！########## */}
                        <div style={{ minWidth: 1200 }}>
                            <UniDataGrid
                                sx={{
                                    width: "100%",
                                    height: "100%",
                                }}
                                rows={previewRows}
                                columns={
                                    previewRows[0]
                                        ? Object.keys(previewRows[0]).map((key) => ({
                                            field: key,
                                            headerName: key,
                                            width: 130, // 每一列固定宽度，强行撑开
                                        }))
                                        : []
                                }
                                pagination={true}
                                disableColumnMenu
                                disableColumnSorting
                            />
                        </div>
                    </Box>
                </Paper>
            )}
        </Box>
    );
}