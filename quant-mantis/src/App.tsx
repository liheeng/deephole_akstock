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
// Init nodes
import { initRegisteredNodes } from "./api/client";
initRegisteredNodes();
// import { NodeRegistry } from "./model/dsl_node/node_registry";
// import { useNodes } from "./hooks/useNodes"
// import { apiClient } from "../../api/client";
// NodeRegistry.fromDict(useNodes())

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