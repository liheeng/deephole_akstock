import { AppModule } from "../../core/moduleRegistry"
import { TasksMonitorPage } from './TasksMonitor'
import TaskDetail from './TaskDetail'
import ScriptExecutorPage from './SyncStockDailyPage'

import AssignmentOutlined from "@mui/icons-material/AssignmentOutlined"

export const taskModule = {
  name: "task",

  menu: [
    {
      text: "任务中心",
      children: [
        { text: "任务监视", path: "/tasks_monitor" },
        { text: "同步日线数据", path: "/sync_daily" },
        { text: "Script Executor", path: "/script_executor" }
      ]
    }
  ],

  routes: [
    { path: "/tasks_monitor", element: <TasksMonitorPage /> },
    { path: "/sync_daily", element: <SyncStockDailyPage /> },
    { path: "/script_executor", element: <ScriptExecutorPage /> },

    // 内部
    { path: "/task/:id", element: <TaskDetail /> }
  ]
}