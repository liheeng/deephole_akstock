import { useState, useRef } from 'react';
import {
  TextField,
  Button,
  Box,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  List,
  ListItemButton,
  ListItemText,
  Stack
} from '@mui/material';

import UniDataGrid from '../../components/table/UniDataGrid';
import { GridToolbar } from '@mui/x-data-grid';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import HistoryIcon from '@mui/icons-material/History';

import MainCard from '../../components/visual/MainCard';
import { apiClient } from "../../api/Client"

export const SqlExecutor = () => {

  const [sql, setSql] = useState('SELECT * FROM stock_daily LIMIT 100;');
  const [data, setData] = useState({ rows: [], columns: [] });

  // ✅ textarea ref（核心）
  const inputRef = useRef<HTMLTextAreaElement | null>(null);

  // ✅ 历史
  const [history, setHistory] = useState<string[]>([]);
  const [openHistory, setOpenHistory] = useState(false);

  // =========================
  // 执行 SQL
  // =========================
  const handleRun = async () => {

    let finalSql = sql;

    // ✅ 获取选中内容
    const el = inputRef.current;
    if (el) {
      const { selectionStart, selectionEnd, value } = el;

      if (selectionStart !== selectionEnd) {
        finalSql = value.slice(selectionStart, selectionEnd);
      }
    }

    if (!finalSql.trim()) return;

    // ✅ 写入历史（去重 + 最新在前）
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
      }
    }
  };

  // =========================
  // 选择历史
  // =========================
  const handleSelectHistory = (item: string) => {
    setSql(item);
    setOpenHistory(false);
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
          inputRef={inputRef} // ✅ 关键
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

          {/* ✅ 历史按钮 */}
          <Button
            variant="outlined"
            startIcon={<HistoryIcon />}
            onClick={() => setOpenHistory(true)}
          >
            历史
          </Button>
        </Stack>

      </MainCard>

      {/* ================= 结果 ================= */}
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

      {/* ================= 历史弹窗 ================= */}
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
              <ListItemButton
                key={i}
                onClick={() => handleSelectHistory(item)}
              >
                <ListItemText
                  primary={item}
                  primaryTypographyProps={{
                    fontFamily: 'Monaco, monospace',
                    fontSize: 13
                  }}
                />
              </ListItemButton>
            ))}
          </List>
        </DialogContent>
      </Dialog>

    </Stack>
  );
};