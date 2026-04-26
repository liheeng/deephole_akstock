import { useState, useRef } from 'react';
import {
  TextField,
  Button,
  Box,
  Dialog,
  DialogTitle,
  DialogContent,
  List,
  ListItemButton,
  Stack
} from '@mui/material';
import ListItemText from '@mui/material/ListItemText';
import UniDataGrid from '../../components/table/UniDataGrid';
import { GridToolbar } from '@mui/x-data-grid';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import HistoryIcon from '@mui/icons-material/History';
import DownloadIcon from "@mui/icons-material/Download";

import MainCard from '../../components/visual/MainCard';
import { apiClient } from "../../api/Client"

export const SqlExecutor = () => {

  const [sql, setSql] = useState('SELECT * FROM stock_daily LIMIT 100');
  const [data, setData] = useState({ rows: [], columns: [] });

  // ✅ textarea ref（用于选中执行）
  const inputRef = useRef<HTMLTextAreaElement | null>(null);

  // ✅ SQL 历史
  const [history, setHistory] = useState<string[]>([]);
  const [openHistory, setOpenHistory] = useState(false);

  // =========================
  // 执行 SQL（核心函数）
  // =========================
  const runSql = async (finalSql: string) => {

    if (!finalSql.trim()) return;

    // 👉 写入历史（去重 + 最新在前）
    setHistory(prev => {
      const next = [finalSql, ...prev.filter(x => x !== finalSql)];
      return next.slice(0, 50);
    });

    const res = await apiClient.post("/execute_sql", { sql: finalSql }, {
      withCredentials: true,
    });

    if (res.data.status === 'success') {
      const rows = res.data.data;

      if (rows.length > 0) {
        const columns = Object.keys(rows[0]).map(key => ({
          field: key,
          headerName: key.toUpperCase(),
          width: 150
        }));

        setData({
          columns: columns as any,
          rows: rows.map((r: any, i: number) => ({ id: i, ...r }))
        });
      } else {
        setData({ rows: [], columns: [] });
      }
    }
  };

  // =========================
  // 点击执行按钮
  // =========================
  const handleRun = async () => {

    let finalSql = sql;

    const el = inputRef.current;

    // ✅ 选中优先
    if (el) {
      const { selectionStart, selectionEnd, value } = el;

      if (selectionStart !== selectionEnd) {
        finalSql = value.slice(selectionStart, selectionEnd);
      }
    }

    runSql(finalSql);
  };

  // =========================
  // 选择历史（回填）
  // =========================
  const handleSelectHistory = (item: string) => {
    setSql(item);
    setOpenHistory(false);
  };

  const exportToCSV = () => {
    if (!data.rows.length) return;

    const headers = data.columns.map((c: any) => c.field);

    const csvRows = [
        headers.join(","), // header
        ...data.rows.map((row: any) =>
        headers.map(h => {
            const val = row[h];

            // 处理逗号、换行、引号
            if (val === null || val === undefined) return "";
            const str = String(val).replace(/"/g, '""');
            return `"${str}"`;
        }).join(",")
        )
    ];

    // ✅ BOM 防止 Excel 中文乱码
    const csvContent = "\uFEFF" + csvRows.join("\n");

    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });

    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;

    // ✅ 文件名带时间
    const now = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    a.download = `sql_result_${now}.csv`;

    a.click();

    URL.revokeObjectURL(url);
    };

  return (
    <Stack
      sx={{
        height: '100vh',
        padding: 2,
        display: 'flex',
        flexDirection: 'column',
        gap: 2
      }}
    >

      {/* ================= SQL 输入 ================= */}
      <MainCard title="SQL 工具">

        <TextField
          fullWidth
          multiline
          rows={5}
          value={sql}
          inputRef={inputRef}
          onChange={(e) => setSql(e.target.value)}
          placeholder="请输入 SQL..."
          sx={{
            mb: 2,
            '& .MuiInputBase-input': {
              fontFamily: 'Monaco, monospace'
            }
          }}
        />

        <Stack direction="row" spacing={1}>
          <Button
            variant="contained"
            startIcon={<PlayArrowIcon />}
            onClick={handleRun}
          >
            执行
          </Button>

          <Button
            variant="outlined"
            startIcon={<HistoryIcon />}
            onClick={() => setOpenHistory(true)}
          >
            历史
          </Button>

          {/* ✅ 新增导出 */}
            <Button
                variant="outlined"
                startIcon={<DownloadIcon />}
                onClick={exportToCSV}
                disabled={!data.rows.length}   // 没数据不可点
            >
                导出 CSV
            </Button>
        </Stack>

      </MainCard>

      {/* ================= 查询结果 ================= */}
      {data.rows.length > 0 && (
        <MainCard content={false} sx={{ flex: 1, minHeight: 0 }}>
          <Box sx={{ height: '100%', width: '100%' }}>
            <UniDataGrid
              sx={{ height: '100%' }}
              rows={data.rows}
              columns={data.columns}
              slots={{ toolbar: GridToolbar }}
              density="compact"
            />
          </Box>
        </MainCard>
      )}

      {/* ================= SQL 历史 ================= */}
      <Dialog
        open={openHistory}
        onClose={() => setOpenHistory(false)}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>SQL 历史</DialogTitle>

        <DialogContent>
          <List dense>

            {history.map((item, i) => (
              <Box
                key={i}
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1,
                  borderBottom: "1px solid rgba(255,255,255,0.05)"
                }}
              >

                {/* 点击：回填 */}
                <ListItemButton
                  sx={{ flex: 1 }}
                  onClick={() => handleSelectHistory(item)}
                  onDoubleClick={() => {
                    runSql(item)
                    setOpenHistory(false)
                  }}
                >
                  <ListItemText
                    primary={item}
                    primaryTypographyProps={{
                      fontFamily: 'Monaco, monospace',
                      fontSize: 13,
                      noWrap: true
                    }}
                  />
                </ListItemButton>

                {/* 点击：直接执行 */}
                <Button
                  size="small"
                  variant="outlined"
                  onClick={() => {
                    runSql(item);
                    setOpenHistory(false);
                  }}
                >
                  执行
                </Button>

              </Box>
            ))}

          </List>
        </DialogContent>
      </Dialog>

    </Stack>
  );
};