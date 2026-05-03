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
  TableBody
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { useQuery } from '@tanstack/react-query';
import { apiClient } from "../../api/Client";


const jobTypeMap = {
    cn_daily_sync: "A股日线同步",
    hk_daily_sync: "港股日线同步",
    us_daily_sync: "美股日线同步",
};

const fmt = (t?: string) =>
  t ? new Date(t).toLocaleString() : '-';

export default function TasksMonitorPage() {
  const [autoRefresh, setAutoRefresh] = useState(true);

  const { data: tasks, isFetching } = useQuery({
    queryKey: ['tasks'],
    queryFn: () =>
      apiClient.get('/tasks', { withCredentials: true }).then((res) => {
        if (res.status !== 200) return [];
        return res.data;
      }),
    refetchInterval: autoRefresh ? 5000 : false
  });

  const getStatusConfig = (status: string) => {
    const s = status?.toUpperCase();
    if (s === 'SUCCESS') return { color: 'success', label: '成功' };
    if (s === 'RUNNING') return { color: 'warning', label: '运行中' };
    if (s === 'FAILED') return { color: 'error', label: '失败' };
    return { color: 'default', label: status };
  };

  return (
    <Stack spacing={3}>
      {/* Header */}
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

      {/* Loading */}
      {isFetching && (
        <LinearProgress color="primary" sx={{ height: 2, borderRadius: 1 }} />
      )}

      {/* Task List */}
      {tasks?.map((task: any) => {
        const config = getStatusConfig(task.status);

        return (
          <Accordion key={task.id} variant="outlined" sx={{ borderRadius: 2 }}>
            {/* Task Header */}
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              {/* <Stack direction="row" spacing={2} alignItems="center">
                <Chip size="small" label={config.label} color={config.color as any} />
                <Typography variant="subtitle1" fontSize={14}>任务ID: {task.id}</Typography>
                <Typography variant="body2" color="text.secondary" fontSize={16}>
                  {task.description}
                </Typography>
              </Stack> */}
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

            {/* Task Detail */}
            <AccordionDetails sx={{ px: 2 }}>
              {/* Task Info */}
              <Box sx={{ textAlign: "left" }}>
                <Typography sx={{ display: 'block', variant: 'caption', fontSize: 14 }}>
                  开始时间: {task.start_time} | 结束时间: {task.stop_time}
                </Typography>
                {/* <Typography sx={{ display: 'block', variant: 'caption' }}>
                  结束时间: {task.stop_time}
                </Typography> */}
                <Typography sx={{ display: 'block', variant: 'caption', fontSize: 14 }}>
                  消息: {task.message || '无'}
                </Typography>
              </Box>

              {/* Jobs Table */}
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
  );
};