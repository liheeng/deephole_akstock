import {
    Box, MenuItem, Select, Typography, Dialog, DialogTitle,
    DialogContent, TextField, DialogActions, Button, Tabs, Tab
} from "@mui/material";
import { useEffect, useState } from "react";
import WebTerminal from "../components/terminal/WebTerminal";
import { fetchTerminalTargets } from "../api/Client";

type Target = {
    id: string;
    name: string;
    type: string;
    mode?: string;
};

// Add a unique instance ID to tabs so you can open the same target twice if needed
// type TabInstance = Target & { instanceId: string; host?: string; username?: string; password?: string };

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

    // 1. 只在初始化时获取列表
    useEffect(() => {
        fetchTerminalTargets().then(res => {
            setTargets(res);
        });
    }, []);

    // 2. 移除之前会自动 handleSelect(first) 的那个 useEffect，防止干扰

    const handleSelect = (id: string) => {
        if (!id) return; // 忽略空值

        const t = targets.find(x => x.id === id);
        if (!t) return;

        // 如果是 server1 或者是 ssh 类型
        if (t.id === "server1" || (t.type === "host" && t.mode === "ssh")) {
            setSelectedId(id); // 记录当前选的是哪个，给 handleLaunch 用
            setOpenLauncher(true);
        } else {
            addTab(t, t.name);
            setSelectedId(""); // 普通目标选完直接重置下拉框
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
            // 使用用户输入的 IP 地址作为 Tab 标题
            addTab(t, form.host || t.name, form);
        }
        setOpenLauncher(false);
        setForm({ host: "", username: "", password: "" });
        setSelectedId(""); // 弹窗结束后重置下拉框
    };

    const closeTab = (instanceId: string, e: React.MouseEvent) => {
        e.stopPropagation();
        setTabs(prev => {
            const nextTabs = prev.filter(t => t.instanceId !== instanceId);
            // 如果关闭的是当前 Tab，切换到最后一个
            if (activeTabId === instanceId) {
                setActiveTabId(nextTabs.length > 0 ? nextTabs[nextTabs.length - 1].instanceId : null);
            }
            return nextTabs;
        });
    };

    return (
        <Box sx={{ p: 2 }}>
            <Typography variant="h5" sx={{ mb: 2 }}>🖥️ Web Terminal</Typography>

            {/* 下拉选择框 */}
            <Select
                size="small"
                value={selectedId} // 关键：选完后会被重置为 ""，所以能重复触发
                displayEmpty
                onChange={(e) => handleSelect(e.target.value)}
                sx={{ mb: 2, minWidth: 260 }}
            >
                <MenuItem value="" disabled>+ 点击此处打开新终端</MenuItem>
                {targets.map(t => (
                    <MenuItem key={t.id} value={t.id}>
                        {t.name} {t.id === 'server1' ? '(可自定义IP)' : ''}
                    </MenuItem>
                ))}
            </Select>

            {/* Tab 标签栏 */}
            {tabs.length > 0 && (
                <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
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
                                                '&:hover': { color: 'primary.main', fontWeight: 'bold' }
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

            {/* 终端内容区 */}
            <Box sx={{ mt: 1 }}>
                {tabs.map((tab) => (
                    <Box
                        key={tab.instanceId}
                        sx={{ display: activeTabId === tab.instanceId ? 'block' : 'none' }}
                    >
                        <WebTerminal target={tab as any} />
                    </Box>
                ))}
            </Box>

            {/* SSH Dialog remains same as your original */}
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