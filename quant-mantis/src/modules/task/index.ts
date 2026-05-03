import React from "react"
import TasksMonitorPage from './TasksMonitor'
import TaskDetail from './sections/TaskDetail'
import SyncStockDailyPage from './SyncStockDailyPage'
import { AssignmentOutlined } from "@mui/icons-material"
import ScriptExecutorPage from './ScriptExecutorPage'
import EnhancedTaskMonitorPage from './EnhancedTaskMonitorPage'
// import AssignmentOutlined from "@mui/icons-material/AssignmentOutlined"

export const taskModule = {
  name: "task",

  menu: [
    {
      text: "任务中心",
      icon: React.createElement(AssignmentOutlined),
      children: [
        { text: "增强任务监视", path: "/tasks_monitor_v2" },
        { text: "任务监视", path: "/tasks_monitor" },
        { text: "同步日线数据", path: "/sync_daily" },
        { text: "Script Executor", path: "/script_executor" }
      ]
    }
  ],

  routes: [
    { path: "/tasks_monitor_v2", element: React.createElement(EnhancedTaskMonitorPage) },
    { path: "/tasks_monitor", element: React.createElement(TasksMonitorPage) },
    { path: "/sync_daily", element: React.createElement(SyncStockDailyPage) },
    { path: "/script_executor", element: React.createElement(ScriptExecutorPage) },

    // 内部
    { path: "/task/:id", element: React.createElement(TaskDetail) }
  ]
}