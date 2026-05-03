import { useEffect, useState } from "react";
import { BrowserRouter, Link, useLocation } from "react-router-dom";
import {
    Box, CssBaseline, ThemeProvider, createTheme,
    Drawer, AppBar, Toolbar, List, Typography,
    ListItem, ListItemButton, ListItemIcon, ListItemText, IconButton
} from "@mui/material";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider"
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs"
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { initRegisteredNodes } from "./api/Client";
import MenuOpenIcon from "@mui/icons-material/MenuOpen";
import MenuIcon from "@mui/icons-material/Menu";
import { useJupyterLabStore } from './store/jupyterlab.store';
import { stopJupyterLab } from './api/Client'

import { Collapse } from "@mui/material";
import { buildMenu } from "./core/buildMenu"
import { BuildRoutes } from "./core/buildRoutes"


const theme = createTheme({
    palette: {
        mode: "dark",
        primary: { main: '#1890ff' }
    }
});

// const menuItems = [
//     { text: '仪表盘', icon: <DashboardOutlined />, path: '/' },
//     { text: '量化回测', icon: <AnalyticsOutlined />, path: '/backtest' },
//     { text: 'Portfolio Expert', icon: <TerminalIcon />, path: '/portfolio_expert' },
//     { text: 'Jupyter Lab', icon: <TerminalIcon />, path: '/jupyter_lab' },
//     { text: 'SQL执行器', icon: <StorageOutlined />, path: '/sql_executor' },
//     { text: '同步日线数据', icon: <StorageOutlined />, path: '/sync_daily' },
//     { text: '任务监视', icon: <AssignmentOutlined />, path: '/tasks_monitor' },
//     { text: '导出数据', icon: <StorageOutlined />, path: '/export_data' },
//     { text: 'Web终端', icon: <TerminalIcon />, path: '/terminal' },
// ];

function MainLayout({ children }: { children: React.ReactNode }) {
    const location = useLocation();
    const [drawerCollapsed, setDrawerCollapsed] = useState(false);
    const [openGroup, setOpenGroup] = useState<string | null>(null);

    const menuGroups = buildMenu(); // ✅ 用模块系统生成

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
                        {menuGroups.map((group) => {
                            const isOpen = openGroup === group.text;
                            const hasChildren = group.children && group.children.length > 0;

                            return (
                                <>
                                    {/* 一级菜单 */}
                                    <ListItem disablePadding>
                                        <ListItemButton
                                            component={hasChildren ? "div" : Link}
                                            to={hasChildren ? undefined : group.path}
                                            onClick={() => {
                                                if (hasChildren) {
                                                    setOpenGroup(isOpen ? null : group.text);
                                                }
                                            }}
                                            selected={!hasChildren && location.pathname === group.path}
                                        >
                                            <ListItemIcon>{group.icon}</ListItemIcon>
                                            <ListItemText primary={group.text} />

                                            {/* 👇 这里加展开/收起箭头 */}
                                            {hasChildren && (
                                                isOpen ? <ExpandLessIcon /> : <ExpandMoreIcon />
                                            )}

                                        </ListItemButton>
                                    </ListItem>

                                    {/* 二级菜单 */}
                                    <Collapse in={isOpen} timeout="auto" unmountOnExit>
                                        <List disablePadding>
                                            {group.children?.map((item) => (
                                                <ListItem key={item.path} disablePadding>
                                                    <ListItemButton
                                                        component={Link}
                                                        to={item.path!}
                                                        selected={location.pathname === item.path}
                                                        sx={{ pl: 4 }}
                                                    >
                                                        <ListItemText primary={item.text} />
                                                    </ListItemButton>
                                                </ListItem>
                                            ))}
                                        </List>
                                    </Collapse>
                                </>
                            );
                        })}
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
    const updateJupyterStatus = useJupyterLabStore(s => s.updateStatus);

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

        const stopJupyter = async () => {
            try {
                const data = await stopJupyterLab()
                updateJupyterStatus(data.process_id, data.status)
                console.log(`Jupyter stoped, process id: ${data.process_id}`)
            } catch (err) {
                console.error('停止失败', err)
            }
        }

        // 监听关闭/刷新
        window.addEventListener('beforeunload', stopJupyter);

        // 组件卸载（路由跳转也会触发）
        return () => {
            window.removeEventListener('beforeunload', stopJupyter);
        };

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
                            <BuildRoutes />
                        </MainLayout>
                    </BrowserRouter>
                </QueryClientProvider>
            </ThemeProvider>
        </LocalizationProvider>
    );
}