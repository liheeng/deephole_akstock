import { useEffect, useState } from "react";
import { BrowserRouter, Routes, Route, Link, useLocation } from "react-router-dom";
import {
    Box, CssBaseline, ThemeProvider, createTheme,
    Drawer, AppBar, Toolbar, List, Typography,
    Divider, ListItem, ListItemButton, ListItemIcon, ListItemText
} from "@mui/material";

// 图标导入
import {
    DashboardOutlined,
    AnalyticsOutlined,
    StorageOutlined,
    AssignmentOutlined
} from "@mui/icons-material";

// 页面组件导入
import BacktestPage from "./pages/BacktestPage";
import { ExportDataPage } from "./pages/stock/ExportData";
import { SqlExecutor } from "./pages/stock/SqlExecutor";
import { TasksMonitorPage } from "./pages/stock/TasksMonitor";
import { SyncStockDailyPage } from "./pages/stock/SyncStockDailyPage";

import { initMonacoEnv } from "./monacoEnv";

// 👇 加上这两行！！！
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const drawerWidth = 240;

const theme = createTheme({
    palette: {
        mode: "dark",
        primary: { main: '#1890ff' }
    }
});

// 侧边栏菜单配置
const menuItems = [
    { text: '仪表盘', icon: <DashboardOutlined />, path: '/' },
    { text: '回测系统', icon: <AnalyticsOutlined />, path: '/backtest' },
    { text: '同步日线数据', icon: <StorageOutlined />, path: '/sync_daily' },
    { text: 'SQL执行器', icon: <StorageOutlined />, path: '/sql_executor' },
    { text: '任务监视', icon: <AssignmentOutlined />, path: '/tasks_monitor' },
    { text: '导出数据', icon: <StorageOutlined />, path: '/export_data' },
];

// 布局组件：包含侧边栏和顶部栏
function MainLayout({ children }: { children: React.ReactNode }) {
    const location = useLocation();

    return (
        <Box sx={{ display: 'flex' }}>
            <CssBaseline />
            {/* 顶部标题栏 */}
            <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
                <Toolbar>
                    <Typography variant="h6" noWrap component="div">
                        📊 Stock Back Testing Dashboard
                    </Typography>
                </Toolbar>
            </AppBar>

            {/* 侧边栏 */}
            <Drawer
                variant="permanent"
                sx={{
                    width: drawerWidth,
                    flexShrink: 0,
                    [`& .MuiDrawer-paper`]: { width: drawerWidth, boxSizing: 'border-box' },
                }}
            >
                <Toolbar />
                <Box sx={{ overflow: 'auto' }}>
                    <List>
                        {menuItems.map((item) => (
                            <ListItem key={item.text} disablePadding>
                                <ListItemButton
                                    component={Link}
                                    to={item.path}
                                    selected={location.pathname === item.path}
                                >
                                    <ListItemIcon sx={{ color: location.pathname === item.path ? 'primary.main' : 'inherit' }}>
                                        {item.icon}
                                    </ListItemIcon>
                                    <ListItemText primary={item.text} />
                                </ListItemButton>
                            </ListItem>
                        ))}
                    </List>
                </Box>
            </Drawer>

            {/* 主内容区域 */}
            <Box component="main" sx={{ flexGrow: 1, p: 3, mt: 8 }}>
                {children}
            </Box>
        </Box>
    );
}

export default function App() {
    // 👇 加上这一行！！！
    const queryClient = new QueryClient();

    useEffect(() => {
        initMonacoEnv();
    }, []);

    return (
        <ThemeProvider theme={theme}>
            {/* 👇 关键：用 QueryClientProvider 把整个应用包起来 */}
            <QueryClientProvider client={queryClient}>
                <BrowserRouter>
                    <MainLayout>
                        <Routes>
                            <Route path="/" element={<Typography variant="h4">欢迎来到DeepHole股票回测系统</Typography>} />
                            <Route path="/backtest" element={<BacktestPage />} />
                            {/* <Route path="/sql" element={<Typography variant="h4">SQL 执行器界面 (待开发)</Typography>} /> */}
                            <Route path="/sync_daily" element={<SyncStockDailyPage />} />
                            <Route path="/sql_executor" element={<SqlExecutor />} />
                            {/* <Route path="/tasks_monitor" element={<Typography variant="h4">任务管理界面 (待开发)</Typography>} /> */}
                            <Route path="/tasks_monitor" element={<TasksMonitorPage />} />
                            {/* <Route path="/export" element={<Typography variant="h4">任务管理界面 (待开发)</Typography>} /> */}
                            <Route path="/export_data" element={<ExportDataPage />} />
                            {/* 可以在这里添加更多路由 */}
                        </Routes>
                    </MainLayout>
                </BrowserRouter>
            </QueryClientProvider>
        </ThemeProvider>
    );
}


// import { CssBaseline, ThemeProvider, createTheme } from "@mui/material"
// import BacktestPage from "./pages/BacktestPage"
// import { initMonacoEnv } from "./monacoEnv"
// import { useEffect } from "react"
// import { NodeRegistry } from "./model/dsl_node/node_registry"
// import { useNodes } from "./hooks/useNodes"

// NodeRegistry.fromDict(useNodes())
// console.log(NodeRegistry.toDict())

// const theme = createTheme({
//   palette: {
//     mode: "dark"
//   }
// })

// export default function App() {
//     useEffect(() => {
//         initMonacoEnv()
//     }, [])


//   return (
//     <ThemeProvider theme={theme}>
//       <CssBaseline />
//       <BacktestPage />
//     </ThemeProvider>
//   )
// }

// import { useState } from 'react'
// import reactLogo from './assets/react.svg'
// import viteLogo from './assets/vite.svg'
// import heroImg from './assets/hero.png'
// import './App.css'

// function App() {
//   const [count, setCount] = useState(0)

//   return (
//     <>
//       <section id="center">
//         <div className="hero">
//           <img src={heroImg} className="base" width="170" height="179" alt="" />
//           <img src={reactLogo} className="framework" alt="React logo" />
//           <img src={viteLogo} className="vite" alt="Vite logo" />
//         </div>
//         <div>
//           <h1>Get started</h1>
//           <p>
//             Edit <code>src/App.tsx</code> and save to test <code>HMR</code>
//           </p>
//         </div>
//         <button
//           className="counter"
//           onClick={() => setCount((count) => count + 1)}
//         >
//           Count is {count}
//         </button>
//       </section>

//       <div className="ticks"></div>

//       <section id="next-steps">
//         <div id="docs">
//           <svg className="icon" role="presentation" aria-hidden="true">
//             <use href="/icons.svg#documentation-icon"></use>
//           </svg>
//           <h2>Documentation</h2>
//           <p>Your questions, answered</p>
//           <ul>
//             <li>
//               <a href="https://vite.dev/" target="_blank">
//                 <img className="logo" src={viteLogo} alt="" />
//                 Explore Vite
//               </a>
//             </li>
//             <li>
//               <a href="https://react.dev/" target="_blank">
//                 <img className="button-icon" src={reactLogo} alt="" />
//                 Learn more
//               </a>
//             </li>
//           </ul>
//         </div>
//         <div id="social">
//           <svg className="icon" role="presentation" aria-hidden="true">
//             <use href="/icons.svg#social-icon"></use>
//           </svg>
//           <h2>Connect with us</h2>
//           <p>Join the Vite community</p>
//           <ul>
//             <li>
//               <a href="https://github.com/vitejs/vite" target="_blank">
//                 <svg
//                   className="button-icon"
//                   role="presentation"
//                   aria-hidden="true"
//                 >
//                   <use href="/icons.svg#github-icon"></use>
//                 </svg>
//                 GitHub
//               </a>
//             </li>
//             <li>
//               <a href="https://chat.vite.dev/" target="_blank">
//                 <svg
//                   className="button-icon"
//                   role="presentation"
//                   aria-hidden="true"
//                 >
//                   <use href="/icons.svg#discord-icon"></use>
//                 </svg>
//                 Discord
//               </a>
//             </li>
//             <li>
//               <a href="https://x.com/vite_js" target="_blank">
//                 <svg
//                   className="button-icon"
//                   role="presentation"
//                   aria-hidden="true"
//                 >
//                   <use href="/icons.svg#x-icon"></use>
//                 </svg>
//                 X.com
//               </a>
//             </li>
//             <li>
//               <a href="https://bsky.app/profile/vite.dev" target="_blank">
//                 <svg
//                   className="button-icon"
//                   role="presentation"
//                   aria-hidden="true"
//                 >
//                   <use href="/icons.svg#bluesky-icon"></use>
//                 </svg>
//                 Bluesky
//               </a>
//             </li>
//           </ul>
//         </div>
//       </section>

//       <div className="ticks"></div>
//       <section id="spacer"></section>
//     </>
//   )
// }

// export default App
