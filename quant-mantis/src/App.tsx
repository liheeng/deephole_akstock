import { useEffect, useState } from "react";
import { BrowserRouter, Routes, Route, Link, useLocation } from "react-router-dom";
import {
    Box, CssBaseline, ThemeProvider, createTheme,
    Drawer, AppBar, Toolbar, List, Typography,
    Divider, ListItem, ListItemButton, ListItemIcon, ListItemText
} from "@mui/material";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider"
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs"
import {
    DashboardOutlined,
    AnalyticsOutlined,
    StorageOutlined,
    AssignmentOutlined
} from "@mui/icons-material";

import BacktestPage from "./pages/BacktestPage";
import { ExportDataPage } from "./pages/stock/ExportData";
import { SqlExecutor } from "./pages/stock/SqlExecutor";
import { TasksMonitorPage } from "./pages/stock/TasksMonitor";
import { SyncStockDailyPage } from "./pages/stock/SyncStockDailyPage";

import { initMonacoEnv } from "./monacoEnv";

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { initRegisteredNodes } from "./api/Client";

const drawerWidth = 240;

const theme = createTheme({
    palette: {
        mode: "dark",
        primary: { main: '#1890ff' }
    }
});

const menuItems = [
    { text: '仪表盘', icon: <DashboardOutlined />, path: '/' },
    { text: '回测管理', icon: <AnalyticsOutlined />, path: '/backtest' },
    { text: '同步日线数据', icon: <StorageOutlined />, path: '/sync_daily' },
    { text: 'SQL执行器', icon: <StorageOutlined />, path: '/sql_executor' },
    { text: '任务监视', icon: <AssignmentOutlined />, path: '/tasks_monitor' },
    { text: '导出数据', icon: <StorageOutlined />, path: '/export_data' },
];

function MainLayout({ children }: { children: React.ReactNode }) {
    const location = useLocation();

    return (
        <Box sx={{ display: 'flex' }}>
            <CssBaseline />
            <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
                <Toolbar>
                    <Typography variant="h6" noWrap component="div">
                        📊 Stock Back Testing Dashboard
                    </Typography>
                </Toolbar>
            </AppBar>

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

            <Box component="main" sx={{ flexGrow: 1, p: 3, mt: 8 }}>
                {children}
            </Box>
        </Box>
    );
}

export default function App() {
    const queryClient = new QueryClient();
    // 👇 控制初始化完成才渲染页面
    const [ready, setReady] = useState(false);

    useEffect(() => {
        const initAll = async () => {
            try {
                // 1. 先初始化节点（必须等它！）
                await initRegisteredNodes();
                // 2. 再初始化编辑器
                initMonacoEnv();
            } catch (err) {
                console.error("初始化失败", err);
            } finally {
                // 3. 全部做完，才允许渲染页面
                setReady(true);
            }
        };

        initAll();
    }, []);

    // 👇 初始化没完成，显示加载中
    if (!ready) {
        return (
            <ThemeProvider theme={theme}>
                <Box sx={{
                    height: '100vh',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                }}>
                    <Typography variant="h5">
                        ⏳ 正在初始化系统...
                    </Typography>
                </Box>
            </ThemeProvider>
        );
    }

    return (
        <LocalizationProvider dateAdapter={AdapterDayjs}>
            <ThemeProvider theme={theme}>
                <QueryClientProvider client={queryClient}>
                    <BrowserRouter>
                        <MainLayout>
                            <Routes>
                                <Route path="/" element={<Typography variant="h4">欢迎来到DeepHole股票回测系统</Typography>} />
                                <Route path="/backtest" element={<BacktestPage />} />
                                <Route path="/sync_daily" element={<SyncStockDailyPage />} />
                                <Route path="/sql_executor" element={<SqlExecutor />} />
                                <Route path="/tasks_monitor" element={<TasksMonitorPage />} />
                                <Route path="/export_data" element={<ExportDataPage />} />
                            </Routes>
                        </MainLayout>
                    </BrowserRouter>
                </QueryClientProvider>
            </ThemeProvider>
        </LocalizationProvider>
    );
}