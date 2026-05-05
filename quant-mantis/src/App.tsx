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

function MainLayout({ children }: { children: React.ReactNode }) {
    const location = useLocation();
    const [drawerCollapsed, setDrawerCollapsed] = useState(false);
    const [openGroup, setOpenGroup] = useState<string | null>(null);

    const menuGroups = buildMenu();

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
                                            <ListItemIcon sx={{ minWidth: '40px', justifyContent: 'center' }}>
                                                {group.icon}
                                            </ListItemIcon>

                                            {/* 最小化时不显示文字 */}
                                            {!drawerCollapsed && (
                                                <ListItemText primary={group.text} sx={{ m: 0 }} />
                                            )}

                                            {/* 最小化时不显示箭头 */}
                                            {!drawerCollapsed && hasChildren && (
                                                isOpen ? <ExpandLessIcon /> : <ExpandMoreIcon />
                                            )}
                                        </ListItemButton>
                                    </ListItem>

                                    {/* 二级菜单：最小化时直接隐藏 */}
                                    {!drawerCollapsed && (
                                        <Collapse in={isOpen} timeout="auto" unmountOnExit>
                                            <List disablePadding>
                                                {group.children?.map((item: any) => (
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
                                    )}
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
                    overflow: "hidden",
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

        window.addEventListener('beforeunload', stopJupyter);
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
                            <BuildRoutes>
                            </BuildRoutes>
                        </MainLayout>
                    </BrowserRouter>
                </QueryClientProvider>
            </ThemeProvider>
        </LocalizationProvider>
    );
}