import React, { useState } from "react";
import {
  Box,
  Tabs,
  Tab,
  Select,
  MenuItem,
  Button,
  Typography,
  Alert,
  CircularProgress,
  Paper
} from "@mui/material";
import axios from "axios";

import { apiClient } from "../../api/Client_s";

const DATA_SOURCES = [
  "IFIND_API",
  "AKSHARE_SINA_API"
];

const MARKET_CONFIG = {
  CN: "cn_daily_sync",
  HK: "hk_daily_sync",
  US: "us_daily_sync"
} as const;

type Market = keyof typeof MARKET_CONFIG;

export function SyncStockDailyPage() {
  const [market, setMarket] = useState<Market>("CN");
  const [dataSource, setDataSource] = useState(DATA_SOURCES[0]);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const handleSync = async () => {
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const res = await apiClient.get(
        `/sync_daily/${MARKET_CONFIG[market]}`,
        {
            params: { data_source_api: dataSource },
            withCredentials: true
        }
      );
      setResult(res.data);
    } catch (err: any) {
      setError(err?.response?.data || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box p={3}>
      <Typography variant="h5" gutterBottom>
        📊 同步日线数据
      </Typography>

      {/* 市场切换 */}
      <Tabs
        value={market}
        onChange={(e, v) => {
          setMarket(v);
          setResult(null);
          setError(null);
        }}
        sx={{ mb: 2 }}
      >
        <Tab label="🇨🇳 CN" value="CN" />
        <Tab label="🇭🇰 HK" value="HK" />
        <Tab label="🇺🇸 US" value="US" />
      </Tabs>

      {/* 数据源 */}
      <Box mb={2}>
        <Select
          size="small"
          value={dataSource}
          onChange={(e) => setDataSource(e.target.value)}
        >
          {DATA_SOURCES.map((ds) => (
            <MenuItem key={ds} value={ds}>
              {ds}
            </MenuItem>
          ))}
        </Select>
      </Box>

      {/* 按钮 */}
      <Box mb={2}>
        <Button
          variant="contained"
          onClick={handleSync}
          disabled={loading}
          startIcon={loading && <CircularProgress size={16} />}
        >
          🚀 执行同步（{market}）
        </Button>
      </Box>

      {/* 错误 */}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          ❌ {error}
        </Alert>
      )}

      {/* 成功 */}
      {result && (
        <Alert severity="success" sx={{ mb: 2 }}>
          ✅ 任务触发成功 | 市场：{market} | 数据源：{dataSource}
        </Alert>
      )}

      {/* 返回结果 */}
      {result && (
        <Paper sx={{ p: 2, background: "#111", color: "#0f0" }}>
          <pre style={{ margin: 0 }}>
            {JSON.stringify(result, null, 2)}
          </pre>
        </Paper>
      )}
    </Box>
  );
}