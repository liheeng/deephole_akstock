import { useEffect, useState } from "react";
import { BrowserRouter, Routes, Route, Link, useLocation } from "react-router-dom";
import {
    Box, CssBaseline, ThemeProvider, createTheme,
    Drawer, AppBar, Toolbar, List, Typography,
    ListItem, ListItemButton, ListItemIcon, ListItemText, IconButton,
    Tooltip  // 👈 导入 Tooltip
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

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { initRegisteredNodes } from "./api/Client";
import MenuOpenIcon from "@mui/icons-material/MenuOpen";
import MenuIcon from "@mui/icons-material/Menu";
import TerminalIcon from "@mui/icons-material/Terminal";
import TerminalPage from "./pages/TerminalPage";
import PortfolioExpertPage from "./pages/PortfolioExpertPage"


const theme = createTheme({
    palette: {
        mode: "dark",
        primary: { main: '#1890ff' }
    }
});

const menuItems = [
    { text: '仪表盘', icon: <DashboardOutlined />, path: '/' },
    { text: '量化回测', icon: <AnalyticsOutlined />, path: '/backtest' },
    { text: 'Portfolio Expert', icon: <TerminalIcon />, path: '/portfolio_expert' },
    { text: 'SQL执行器', icon: <StorageOutlined />, path: '/sql_executor' },
    { text: '同步日线数据', icon: <StorageOutlined />, path: '/sync_daily' },
    { text: '任务监视', icon: <AssignmentOutlined />, path: '/tasks_monitor' },
    { text: '导出数据', icon: <StorageOutlined />, path: '/export_data' },
    { text: 'Web终端', icon: <TerminalIcon />, path: '/terminal' },
];

function MainLayout({ children }: { children: React.ReactNode }) {
    const location = useLocation();
    const [drawerCollapsed, setDrawerCollapsed] = useState(false);
    const drawerWidth = drawerCollapsed ? 64 : 240;

    return (
        <Box sx={{ display: 'flex' }}>
            <CssBaseline />
            <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
                <Toolbar>
                    <IconButton onClick={() => setDrawerCollapsed(v => !v)}>
                        {drawerCollapsed ? <MenuIcon /> : <MenuOpenIcon />}
                    </IconButton>

                    <Typography variant="h6" sx={{ ml: 1 }}>
                        📊 Stock Back Testing Dashboard
                    </Typography>
                </Toolbar>
            </AppBar>

            <Drawer
                variant="permanent"
                sx={{
                    width: drawerWidth,
                    flexShrink: 0,
                    "& .MuiDrawer-paper": {
                        width: drawerWidth,
                        boxSizing: "border-box",
                        transition: "width 0.2s ease",
                        overflowX: "hidden",
                    },
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
                                    {/* 🔥 图标 + Tooltip */}
                                    <Tooltip
                                        title={item.text}
                                        placement="right"
                                        arrow
                                        disableHoverListener={!drawerCollapsed}
                                    >
                                        <ListItemIcon
                                            sx={{
                                                color: location.pathname === item.path
                                                    ? 'primary.main'
                                                    : 'inherit'
                                            }}
                                        >
                                            {item.icon}
                                        </ListItemIcon>
                                    </Tooltip>

                                    <ListItemText
                                        primary={drawerCollapsed ? "" : item.text}
                                    />
                                </ListItemButton>
                            </ListItem>
                        ))}
                    </List>
                </Box>
            </Drawer>

<Box
  component="main"
  sx={{
    flexGrow: 1,
    p: 3,
    mt: 8,
    height: "calc(100vh - 64px)",
    overflow: "hidden", // ✅ 完全禁止滚动
    overflowX: "hidden",
  }}
>
  {children}
</Box>
        </Box>
    );
}

export default function App() {
    const queryClient = new QueryClient();
    const [ready, setReady] = useState(false);

    useEffect(() => {
        const initAll = async () => {
            try {
                await initRegisteredNodes();
            } catch (err) {
                console.error("初始化失败", err);
            } finally {
                setReady(true);
            }
        };

        initAll();
    }, []);

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
                                <Route path="/portfolio_expert" element={<PortfolioExpertPage />} />
                                <Route path="/sql_executor" element={<SqlExecutor />} />
                                <Route path="/sync_daily" element={<SyncStockDailyPage />} />
                                <Route path="/tasks_monitor" element={<TasksMonitorPage />} />
                                <Route path="/export_data" element={<ExportDataPage />} />
                                <Route path="/terminal" element={<TerminalPage />} />
                            </Routes>
                        </MainLayout>
                    </BrowserRouter>
                </QueryClientProvider>
            </ThemeProvider>
        </LocalizationProvider>
    );
}