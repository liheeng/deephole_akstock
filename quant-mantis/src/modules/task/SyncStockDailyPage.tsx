import { useState } from 'react';
import {
    Box,
    Typography,
    Accordion,
    AccordionSummary,
    AccordionDetails,
    Chip,
    Stack,
    Switch,
    FormControlLabel,
    LinearProgress,
    Table,
    TableHead,
    TableRow,
    TableCell,
    TableBody,
    Tabs,
    Tab,
    Select,
    MenuItem,
    Button,
    Alert,
    CircularProgress,
    Paper
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { useQuery } from '@tanstack/react-query';
import { apiClient } from "../../api/Client";

// 任务类型映射
const jobTypeMap = {
    cn_daily_sync: "A股日线同步",
    hk_daily_sync: "港股日线同步",
    us_daily_sync: "美股日线同步",
};

// 时间格式化
const fmt = (t?: string) =>
    t ? new Date(t).toLocaleString() : '-';

// 数据源配置
const DATA_SOURCES = [
    "AKSHARE_SINA_API",
    "IFIND_API"
];

// 市场与任务类型映射
const MARKET_CONFIG = {
    CN: "cn_daily_sync",
    HK: "hk_daily_sync",
    US: "us_daily_sync"
} as const;

type Market = keyof typeof MARKET_CONFIG;

export default function SyncStockDailyPage() {
    // 同步任务配置状态
    const [market, setMarket] = useState<Market>("CN");
    const [dataSource, setDataSource] = useState(DATA_SOURCES[0]);
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);

    // 任务监控 - 自动刷新状态
    const [autoRefresh, setAutoRefresh] = useState(true);

    // 任务监控接口请求
    const { data: tasks, isFetching } = useQuery({
        queryKey: ['tasks'],
        queryFn: () =>
            apiClient.get('/tasks', { withCredentials: true }).then((res) => {
                if (res.status !== 200) return [];
                return res.data;
            }),
        refetchInterval: autoRefresh ? 5000 : false
    });

    // 状态样式配置
    const getStatusConfig = (status: string) => {
        const s = status?.toUpperCase();
        if (s === 'SUCCESS') return { color: 'success', label: '成功' };
        if (s === 'RUNNING') return { color: 'warning', label: '运行中' };
        if (s === 'FAILED') return { color: 'error', label: '失败' };
        return { color: 'default', label: status };
    };

    // 执行同步逻辑
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
        <Box sx={{ p: 3 }}>
            {/* ===================== 上半部分：同步日线数据 ===================== */}
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
            <Box sx={{ mb: 2 }}>
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

            {/* 执行按钮 */}
            <Box sx={{ mb: 2 }}>
                <Button
                    variant="contained"
                    onClick={handleSync}
                    disabled={loading}
                    startIcon={loading && <CircularProgress size={16} />}
                >
                    🚀 执行同步（{market}）
                </Button>
            </Box>

            {/* 错误提示 */}
            {error && (
                <Alert severity="error" sx={{ mb: 2 }}>
                    ❌ {error}
                </Alert>
            )}

            {/* 成功提示 */}
            {result && (
                <Alert severity="success" sx={{ mb: 2 }}>
                    ✅ 任务触发成功 | 市场：{market} | 数据源：{dataSource}
                </Alert>
            )}

            {/* 返回结果 */}
            {result && (
                <Paper sx={{ p: 2, background: "#111", color: "#0f0", mb: 4 }}>
                    <pre style={{ margin: 0 }}>
                        {JSON.stringify(result, null, 2)}
                    </pre>
                </Paper>
            )}

            {/* ===================== 下半部分：任务监控中心 ===================== */}
            <Box sx={{ mt: 4 }}>
                <Stack spacing={3}>
                    {/* 监控标题 + 自动刷新 */}
                    <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <Typography variant="h5">📊 任务监控中心</Typography>
                        <FormControlLabel
                            control={
                                <Switch
                                    checked={autoRefresh}
                                    onChange={e => setAutoRefresh(e.target.checked)}
                                />
                            }
                            label="自动刷新"
                        />
                    </Box>

                    {/* 加载进度条 */}
                    {isFetching && (
                        <LinearProgress color="primary" sx={{ height: 2, borderRadius: 1 }} />
                    )}

                    {/* 任务列表 */}
                    {tasks?.map((task: any) => {
                        const config = getStatusConfig(task.status);

                        return (
                            <Accordion key={task.id} variant="outlined" sx={{ borderRadius: 2 }}>
                                {/* 任务头部 */}
                                <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                                    <Stack component="div" direction="row" spacing={2}>
                                        <Chip size="small" label={config.label} color={config.color as any} />
                                        <Typography variant="subtitle1" sx={{ fontSize: 16 }}>
                                            任务ID: {task.id}
                                        </Typography>
                                        <Typography variant="body2" sx={{ color: "text.secondary", fontSize: 18 }}>
                                            {task.description}
                                        </Typography>
                                    </Stack>
                                </AccordionSummary>

                                {/* 任务详情 */}
                                <AccordionDetails sx={{ px: 2 }}>
                                    <Box sx={{ textAlign: "left", mb: 2 }}>
                                        <Typography sx={{ display: 'block', fontSize: 14 }}>
                                            开始时间: {fmt(task.start_time)} | 结束时间: {fmt(task.stop_time)}
                                        </Typography>
                                        <Typography sx={{ display: 'block', fontSize: 14 }}>
                                            消息: {task.message || '无'}
                                        </Typography>
                                    </Box>

                                    {/* 子任务表格 */}
                                    <Table size="small">
                                        <TableHead>
                                            <TableRow>
                                                <TableCell>子任务ID</TableCell>
                                                <TableCell>类型</TableCell>
                                                <TableCell>状态</TableCell>
                                                <TableCell>执行时间</TableCell>
                                                <TableCell>结束时间</TableCell>
                                                <TableCell>重试</TableCell>
                                                <TableCell>信息</TableCell>
                                            </TableRow>
                                        </TableHead>

                                        <TableBody>
                                            {task.jobs?.map((job: any) => {
                                                const jobStatus = getStatusConfig(job.status);

                                                return (
                                                    <TableRow key={job.id}>
                                                        <TableCell>{job.id}</TableCell>
                                                        <TableCell>{(jobTypeMap as any)[job.type] || job.type}</TableCell>

                                                        <TableCell>
                                                            <Chip
                                                                size="small"
                                                                label={jobStatus.label}
                                                                color={jobStatus.color as any}
                                                            />
                                                        </TableCell>

                                                        <TableCell>{fmt(job.execute_time)}</TableCell>
                                                        <TableCell>{fmt(job.stop_time)}</TableCell>

                                                        <TableCell>
                                                            {job.retries}/{job.retry_count}
                                                        </TableCell>

                                                        <TableCell>
                                                            {job.error
                                                                ? <Typography color="error" variant="caption">{job.error}</Typography>
                                                                : job.message || '-'}
                                                        </TableCell>
                                                    </TableRow>
                                                );
                                            })}
                                        </TableBody>
                                    </Table>
                                </AccordionDetails>
                            </Accordion>
                        );
                    })}
                </Stack>
            </Box>
        </Box>
    );
}