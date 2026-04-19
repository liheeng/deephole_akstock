import { useState } from 'react';
import { TextField, Button, Box, Paper } from '@mui/material';
import UniDataGrid from '../../components/table/UniDataGrid';
import { GridToolbar } from '@mui/x-data-grid';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import MainCard from '../../components/visual/MainCard';
import { Stack } from "@mui/material"
import { apiClient } from "../../api/Client"

export const SqlExecutor = () => {
  const [sql, setSql] = useState('SELECT * FROM stock_daily LIMIT 100;');
  const [data, setData] = useState({ rows: [], columns: [] });

  const handleRun = async () => {
    const res = await apiClient.post("/execute_sql", { sql }, {
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

  return (
    // 👇 最外层改成：占满视口高度 + 弹性布局
    <Stack
      sx={{
        height: '100vh',
        spacing: 3,
        padding: 2,
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <MainCard title="SQL 工具">
        <TextField
          fullWidth
          multiline
          rows={5}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          placeholder="请输入 SQL..."
          sx={{ mb: 2, '& .MuiInputBase-input': { fontFamily: 'Monaco, monospace' } }}
        />
        <Button
          variant="contained"
          startIcon={<PlayArrowIcon />}
          onClick={handleRun}
        >
          执行
        </Button>
      </MainCard>

      {data.rows.length > 0 && (
  <MainCard content={false} sx={{ flex: 1, minHeight: 0 }}>
    <Box sx={{ height: 'calc(100vh - 420px)', width: '100%' }}>
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
    </Stack>
  );
};