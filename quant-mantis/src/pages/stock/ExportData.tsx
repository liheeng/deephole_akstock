import React, { useState } from "react";
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

import {apiClient} from "../../api/client"

const ALL_COLS = [
  "symbol", "symbol_name", "market", "date",
  "open", "high", "low", "close", "volume", "amount",
  "pct", "turnover", "adjust_mode", "adjust_factor"
];

export function ExportDataPage() {
  const [selectedCols, setSelectedCols] = useState<string[]>(ALL_COLS);
  const [whereSql, setWhereSql] = useState("");
  const [format, setFormat] = useState<"csv" | "parquet">("csv");

  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);

  const [error, setError] = useState<string | null>(null);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);

  const handleCheck = (col: string) => {
    setSelectedCols((prev) =>
      prev.includes(col)
        ? prev.filter((c) => c !== col)
        : [...prev, col]
    );
  };

  const handleExport = async () => {
    if (selectedCols.length === 0) {
      setError("请至少选择一个字段！");
      return;
    }

    setLoading(true);
    setError(null);
    setSuccessMsg(null);
    setProgress(0);

    try {
      const res = await apiClient.post(
        `/export/stream`,
        {
          columns: selectedCols,
          where_sql: whereSql,
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

      const blob = new Blob([res.data]);
      const url = window.URL.createObjectURL(blob);

      const filename = `stock_daily.${format}`;

      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      a.click();

      window.URL.revokeObjectURL(url);

      setSuccessMsg(`导出完成：${filename}`);

    } catch (err: any) {
      setError(err?.response?.data || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box p={3}>
      <Typography variant="h5" gutterBottom>
        📤 股票数据导出（支持大文件）
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

      {/* WHERE */}
      <TextField
        fullWidth
        label="WHERE 条件"
        placeholder="market='CN' AND date>='2025-01-01'"
        value={whereSql}
        onChange={(e) => setWhereSql(e.target.value)}
        sx={{ mb: 2 }}
      />

      {/* 格式 */}
      <RadioGroup
        row
        value={format}
        onChange={(e) => setFormat(e.target.value as any)}
        sx={{ mb: 2 }}
      >
        <FormControlLabel value="csv" control={<Radio />} label="CSV" />
        <FormControlLabel value="parquet" control={<Radio />} label="Parquet" />
      </RadioGroup>

      {/* 按钮 */}
      <Button
        variant="contained"
        onClick={handleExport}
        disabled={loading}
        startIcon={loading && <CircularProgress size={16} />}
      >
        🚀 开始导出
      </Button>

      {/* 进度条 */}
      {loading && (
        <Box mt={2}>
          <LinearProgress variant="determinate" value={progress} />
          <Typography variant="body2">{progress}%</Typography>
        </Box>
      )}

      {/* 成功 */}
      {successMsg && (
        <Alert severity="success" sx={{ mt: 2 }}>
          ✅ {successMsg}
        </Alert>
      )}

      {/* 错误 */}
      {error && (
        <Alert severity="error" sx={{ mt: 2 }}>
          ❌ {error}
        </Alert>
      )}
    </Box>
  );
}