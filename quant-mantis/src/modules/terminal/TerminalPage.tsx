import {
    Box, MenuItem, Select, Typography, Dialog, DialogTitle,
    DialogContent, TextField, DialogActions, Button, Tabs, Tab
} from "@mui/material";
import { useEffect, useState } from "react";
import WebTerminal from "../../components/terminal/WebTerminal";
import { fetchTerminalTargets } from "../../api/Client";
import { FullScreenBox } from "../../components/misc//FullScreenBox"

type Target = {
    id: string;
    name: string;
    type: string;
    mode?: string;
};

type TabInstance = Target & {
    instanceId: string;
    tabTitle: string;
    host?: string;
    username?: string;
    password?: string;
    fontSize: number;
    themeKey: string;
};

// 预设终端主题
const terminalThemes: Record<string, any> = {
    dark: {
        background: "#1e1e1e",
        foreground: "#d4d4d4",
        cursor: "#ffffff",
        selectionBackground: "#264f78",
    },
    light: {
        background: "#ffffff",
        foreground: "#333333",
        cursor: "#000000",
        selectionBackground: "#c8e1ff",
    },
    midnight: {
        background: "#0c0c0c",
        foreground: "#00ff00",
        cursor: "#00ff00",
        selectionBackground: "#1a331a",
    }
};

export default function TerminalPage() {
    const [targets, setTargets] = useState<Target[]>([]);
    const [selectedId, setSelectedId] = useState("");

    const [tabs, setTabs] = useState<TabInstance[]>([]);
    const [activeTabId, setActiveTabId] = useState<string | null>(null);

    const [openLauncher, setOpenLauncher] = useState(false);
    const [form, setForm] = useState({ host: "", username: "", password: "" });
    const [fullSection, setFullSection] = useState<string | null>(null);
    useEffect(() => {
        fetchTerminalTargets().then(res => {
            setTargets(res);
        });
    }, []);

    const handleSelect = (id: string) => {
        if (!id) return;
        const t = targets.find(x => x.id === id);
        if (!t) return;

        if (t.id === "server1" || (t.type === "host" && t.mode === "ssh")) {
            setSelectedId(id);
            setOpenLauncher(true);
        } else {
            addTab(t, t.name);
            setSelectedId("");
        }
    };

    const addTab = (target: Target, title: string, credentials = {}) => {
        const instanceId = `${target.id}-${Math.random().toString(36).substr(2, 5)}`;
        const newTab: TabInstance = {
            ...target,
            ...credentials,
            instanceId,
            tabTitle: title,
            fontSize: 14,
            themeKey: "dark"
        };
        setTabs(prev => [...prev, newTab]);
        setActiveTabId(instanceId);
    };

    const handleLaunch = () => {
        const t = targets.find(x => x.id === selectedId);
        if (t) {
            addTab(t, form.host || t.name, form);
        }
        setOpenLauncher(false);
        setForm({ host: "", username: "", password: "" });
        setSelectedId("");
    };

    const closeTab = (instanceId: string, e: React.MouseEvent) => {
        e.stopPropagation();
        setTabs(prev => {
            const nextTabs = prev.filter(t => t.instanceId !== instanceId);
            if (activeTabId === instanceId) {
                setActiveTabId(nextTabs.length > 0 ? nextTabs[nextTabs.length - 1].instanceId : null);
            }
            return nextTabs;
        });
    };

    // 修改单个tab字体大小
    const handleChangeFontSize = (instanceId: string, size: number) => {
        setTabs(prev => prev.map(t =>
            t.instanceId === instanceId ? { ...t, fontSize: size } : t
        ));
    };

    // 修改单个tab主题
    const handleChangeTheme = (instanceId: string, themeKey: string) => {
        setTabs(prev => prev.map(t =>
            t.instanceId === instanceId ? { ...t, themeKey } : t
        ));
    };

    return (
        <Box sx={{
            p: 2,
            height: "100vh",
            display: "flex",
            flexDirection: "column"
        }}>
            <Typography variant="h5" sx={{ mb: 2 }}>🖥️ Web Terminal</Typography>

            <Select
                size="small"
                value={selectedId}
                displayEmpty
                onChange={(e) => handleSelect(e.target.value)}
                sx={{ mb: 2, width: 260, maxWidth: 500 }}
            >
                <MenuItem value="" disabled>+ 点击此处打开新终端</MenuItem>
                {targets.map(t => (
                    <MenuItem key={t.id} value={t.id}>
                        {t.name} {t.id === 'server1' ? '(可自定义IP)' : ''}
                    </MenuItem>
                ))}
            </Select>

            <FullScreenBox
                enableIcon={tabs.length > 0}
                isFull={fullSection === 'terminal'}
                onToggle={() => setFullSection(fullSection === 'terminal' ? null : 'terminal')}
                sx={{ flex: 1, minHeight: 0, minWidth: 0 }} // minWidth: 0 允许它被压缩
            >
                {tabs.length > 0 && (
                    <Box sx={{ borderBottom: 1, borderColor: 'divider', flexShrink: 0 }}>
                        <Tabs
                            value={activeTabId}
                            onChange={(_, val) => setActiveTabId(val)}
                            variant="scrollable"
                        >
                            {tabs.map((tab) => (
                                <Tab
                                    key={tab.instanceId}
                                    value={tab.instanceId}
                                    label={
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                            {tab.tabTitle}
                                            <Box
                                                onClick={(e) => closeTab(tab.instanceId, e)}
                                                sx={{
                                                    fontSize: '16px',
                                                    lineHeight: 1,
                                                    '&:hover': { color: 'primary.main', fontWeight: 'bold' },
                                                    cursor: "pointer"
                                                }}
                                            >
                                                ×
                                            </Box>
                                        </Box>
                                    }
                                />
                            ))}
                        </Tabs>
                    </Box>
                )}

                <Box sx={{
                    mt: 1,
                    flex: 1,
                    display: "flex",
                    flexDirection: "column",
                    overflow: "hidden",
                    pb: "20px",
                    boxSizing: "border-box"
                }}>
                    {tabs.map((tab) => (
                        <Box
                            key={tab.instanceId}
                            sx={{
                                display: activeTabId === tab.instanceId ? "flex" : "none",
                                flexDirection: "column",
                                flex: 1,
                                height: "100%",
                                gap: 1
                            }}
                        >
                            {/* 每个终端独立工具栏 */}
                            <Box sx={{
                                display: "flex",
                                alignItems: "center",
                                gap: 1.5,
                                px: 2,
                                py: 1,
                                bgcolor: "#646363",
                                borderRadius: 1,
                                flexShrink: 0
                            }}>
                                <Typography variant="body2" sx={{ color: "#3577fc", width: 60, fontSize: 16 }}>字号:</Typography>
                                {[10, 12, 14, 16, 18, 20, 24, 32].map(size => (
                                    <Button
                                        key={size}
                                        size="small"
                                        sx={{ minWidth: 36 }}
                                        variant={tab.fontSize === size ? "contained" : "outlined"}
                                        onClick={() => handleChangeFontSize(tab.instanceId, size)}
                                    >
                                        {size}
                                    </Button>
                                ))}

                                {/* <Box sx={{ width: 1, height: 20, bgcolor: "#646363", mx: 1 }} /> */}

                                <Typography variant="body2" sx={{ color: "#3577fc", width: 60, fontSize: 16 }}>主题:</Typography>
                                <Button
                                    size="small"
                                    variant={tab.themeKey === "dark" ? "contained" : "outlined"}
                                    onClick={() => handleChangeTheme(tab.instanceId, "dark")}
                                >
                                    暗黑
                                </Button>
                                <Button
                                    size="small"
                                    variant={tab.themeKey === "light" ? "contained" : "outlined"}
                                    onClick={() => handleChangeTheme(tab.instanceId, "light")}
                                >
                                    浅色
                                </Button>
                                <Button
                                    size="small"
                                    variant={tab.themeKey === "midnight" ? "contained" : "outlined"}
                                    onClick={() => handleChangeTheme(tab.instanceId, "midnight")}
                                >
                                    复古绿
                                </Button>
                            </Box>

                            <Box sx={{ flex: 1, overflow: "hidden" }}>
                                <WebTerminal
                                    target={tab as any}
                                    fontSize={tab.fontSize}
                                    theme={terminalThemes[tab.themeKey]}
                                />
                            </Box>
                        </Box>
                    ))}
                </Box>
            </FullScreenBox>
            <Dialog open={openLauncher} onClose={() => setOpenLauncher(false)}>
                <DialogTitle>Connect via SSH</DialogTitle>
                <DialogContent sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 1 }}>
                    <TextField label="Host" value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} />
                    <TextField label="Username" value={form.username} onChange={(e) => setForm({ ...form, username: e.target.value })} />
                    <TextField label="Password" type="password" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} />
                </DialogContent>
                <DialogActions>
                    <Button onClick={() => setOpenLauncher(false)}>Cancel</Button>
                    <Button variant="contained" onClick={handleLaunch}>Connect</Button>
                </DialogActions>
            </Dialog>
        </Box>
    );
}