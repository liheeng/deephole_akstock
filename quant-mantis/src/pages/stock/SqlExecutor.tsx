import { useState } from 'react';
import { TextField, Button, Box, Paper } from '@mui/material';
import { DataGrid, GridToolbar } from '@mui/x-data-grid';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import MainCard from '../../components/visual/MainCard';
// import axios from "axios"
import { Stack } from "@mui/material"
import {apiClient} from "../../api/client"

export const SqlExecutor = () => {
  const [sql, setSql] = useState('SELECT * FROM stock_daily LIMIT 100;');
  const [data, setData] = useState({ rows: [], columns: [] });

  const handleRun = async () => {
    const res = await apiClient.post("/execute_sql", { sql }, {
            withCredentials: true, // 跨域必须加
    });
    // const res = await axios.post('/execute_sql', { sql });
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
    <Stack sx={{spacing:3}}>
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
          执行查询
        </Button>
      </MainCard>

      {data.rows.length > 0 && (
        <MainCard content={false}>
          <Box sx={{ height: 600, width: '100%' }}>
            <DataGrid 
              rows={data.rows} 
              columns={data.columns} 
              slots={{ toolbar: GridToolbar }} // 自带导出和筛选功能
              density="compact"
            />
          </Box>
        </MainCard>
      )}
    </Stack>
  );
};