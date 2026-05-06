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

// 定义 Tab 实例类型，包含 UI 标题
type TabInstance = Target & {
    instanceId: string;
    tabTitle: string; // 用于显示的标题
    host?: string;
    username?: string;
    password?: string;
};

export default function TerminalPage() {
    const [targets, setTargets] = useState<Target[]>([]);
    const [selectedId, setSelectedId] = useState(""); // 下拉框当前选中的 ID

    const [tabs, setTabs] = useState<TabInstance[]>([]);
    const [activeTabId, setActiveTabId] = useState<string | null>(null);

    const [openLauncher, setOpenLauncher] = useState(false);
    const [form, setForm] = useState({ host: "", username: "", password: "" });
    const [fullSection, setFullSection] = useState<string | null>(null);

    // 1. 只在初始化时获取列表
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
            tabTitle: title
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

    return (
        // 最外层：占满整个视口 + 垂直弹性布局
        <Box sx={{
            p: 2,
            height: "100vh",        // 占满屏幕高度
            display: "flex",
            flexDirection: "column"
        }}>
            <Typography variant="h5" sx={{ mb: 2 }}>🖥️ Web Terminal</Typography>

            {/* 下拉选择框 */}
            <Select
                size="small"
                value={selectedId}
                displayEmpty
                onChange={(e) => handleSelect(e.target.value)}
                sx={{ mb: 2, minWidth: 200, width: 300 }}
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
                {/* Tab 标签栏 */}
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

                {/* 终端内容区：自动占满剩余所有空间 */}

                <Box sx={{
                    mt: 1,
                    flex: 1,           // 关键：占满剩余高度
                    display: "flex",
                    flexDirection: "column",
                    pb: "40px",
                    overflow: "hidden" // 防止滚动条异常
                }}>
                    {tabs.map((tab) => (
                        <Box
                            key={tab.instanceId}
                            sx={{
                                display: activeTabId === tab.instanceId ? "flex" : "none",
                                flex: 1,
                                // height: "100%",
                                minHeight: 0
                            }}
                        >

                            <WebTerminal target={tab as any} />

                        </Box>
                    ))}

                </Box>
            </FullScreenBox>
            {/* SSH 弹窗 */}
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