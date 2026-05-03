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

export const TasksMonitorPage = () => {
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

// import React, { useState } from 'react';
// import { 
//   Box, Typography, Accordion, AccordionSummary, AccordionDetails, 
//   Chip, Stack, Switch, FormControlLabel, LinearProgress 
// } from '@mui/material';
// import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
// import { useQuery } from '@tanstack/react-query';
// import MainCard from '../../components/visual/MainCard'; // Mantis 内置的 MUI 包装组件
// import axios from 'axios';
// import {apiClient} from "../../api/client"

// export const TasksMonitorPage = () => {
//   const [autoRefresh, setAutoRefresh] = useState(true);

//   const { data: tasks, isFetching } = useQuery({
//     queryKey: ['tasks'],
//     queryFn: () => apiClient.get('/tasks', { withCredentials: true }).then((res) => {
//         if (res.status != 200)
//             return []
//         return res.data
//     }),
//     refetchInterval: autoRefresh ? 5000 : false
//   });

//   const getStatusConfig = (status: string) => {
//     const s = status.toUpperCase();
//     if (s === 'SUCCESS') return { color: 'success', label: '成功' };
//     if (s === 'RUNNING') return { color: 'info', label: '运行中' };
//     if (s === 'FAILED') return { color: 'error', label: '失败' };
//     return { color: 'default', label: status };
//   };

//   return (
//     <Stack spacing={3}>
//       <Box sx={{display:"flex", justifyContent:"space-between", alignItems:"center"}}>
//         <Typography variant="h5">📊 任务监控中心</Typography>
//         <FormControlLabel
//           control={<Switch checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} />}
//           label="自动刷新"
//         />
//       </Box>
      
//       {isFetching && <LinearProgress color="primary" sx={{ height: 2, borderRadius: 1 }} />}

//       {tasks?.map((task: any) => {
//         const config = getStatusConfig(task.status);
//         return (
//           <Accordion key={task.id} variant="outlined" sx={{ borderRadius: '8px !important' }}>
//             <AccordionSummary expandIcon={<ExpandMoreIcon />}>
//               <Stack sx={{direction:"row", spacing:2, alignItems:"center"}}>
//                 <Chip size="small" label={config.label} color={config.color as any} />
//                 <Typography variant="subtitle1">ID: {task.id}</Typography>
//                 <Typography color="text.secondary" variant="body2">{task.description}</Typography>
//               </Stack>
//             </AccordionSummary>
//             <AccordionDetails>
//   <Box sx={{ bgcolor: 'grey.50', p: 2, borderRadius: 1 }}>

//     {/* Task 信息 */}
//     <Typography variant="caption" display="block">
//       开始时间: {task.start_time}
//     </Typography>

//     <Typography variant="caption" display="block">
//       结束时间: {task.stop_time || '进行中'}
//     </Typography>

//     <Typography variant="caption" display="block">
//       消息: {task.message || '无'}
//     </Typography>

//     {/* Job 列表 */}
//     <Stack spacing={1} sx={{ mt: 1 }}>
//       {task.jobs?.length ? (
//         task.jobs.map((job: any) => {
//           const jobConfig = getStatusConfig(job.status);

//           return (
//             <Box
//               key={job.id}
//               sx={{
//                 p: 1.5,
//                 borderRadius: 1,
//                 border: '1px solid',
//                 borderColor: 'divider',
//                 bgcolor: 'background.paper'
//               }}
//             >
//               {/* 第一行 */}
//               <Stack
//                 sx={{
//                   flexDirection: 'row',
//                   alignItems: 'center',
//                   justifyContent: 'space-between'
//                 }}
//               >
//                 <Stack sx={{ flexDirection: 'row', alignItems: 'center', gap: 1 }}>
//                   <Chip
//                     size="small"
//                     label={jobConfig.label}
//                     color={jobConfig.color as any}
//                   />

//                   {/* 👉 用 type 替代 id（更有意义） */}
//                   <Typography variant="body2" fontWeight={500}>
//                     {jobTypeMap[job.type] || job.type}
//                   </Typography>
//                 </Stack>

//                 <Typography variant="caption" color="text.secondary">
//                   {job.execute_time}
//                 </Typography>
//               </Stack>

//               {/* 第二行：时间 */}
//               <Typography
//                 variant="caption"
//                 sx={{ display: 'block', mt: 0.5, color: 'text.secondary' }}
//               >
//                 执行: {job.execute_time} → {job.stop_time || '进行中'}
//               </Typography>

//               {/* retry 信息 */}
//               <Typography
//                 variant="caption"
//                 sx={{ display: 'block', color: 'text.secondary' }}
//               >
//                 重试: {job.retries}/{job.retry_count}
//               </Typography>

//               {/* message */}
//               {job.message && (
//                 <Typography
//                   variant="caption"
//                   sx={{ display: 'block', mt: 0.5 }}
//                 >
//                   {job.message}
//                 </Typography>
//               )}

//               {/* error（重点） */}
//               {job.error && (
//                 <Typography
//                   variant="caption"
//                   sx={{
//                     display: 'block',
//                     mt: 0.5,
//                     color: 'error.main',
//                     whiteSpace: 'pre-wrap'
//                   }}
//                 >
//                   {job.error}
//                 </Typography>
//               )}
//             </Box>
//           );
//         })
//       ) : (
//         <Typography variant="caption" color="text.secondary">
//           无子任务
//         </Typography>
//       )}
//     </Stack>

//   </Box>
// </AccordionDetails>

//           </Accordion>
//         );
//       })}
//     </Stack>
//   );
// };