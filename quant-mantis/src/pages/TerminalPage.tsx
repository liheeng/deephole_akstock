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
type TabInstance = Target & { instanceId: string; host?: string; username?: string; password?: string };

export default function TerminalPage() {
    const [targets, setTargets] = useState<Target[]>([]);
    const [selectedId, setSelectedId] = useState("");
    
    // Multi-tab state
    const [tabs, setTabs] = useState<TabInstance[]>([]);
    const [activeTabId, setActiveTabId] = useState<string | null>(null);

    const [openLauncher, setOpenLauncher] = useState(false);
    const [form, setForm] = useState({ host: "", username: "", password: "" });

    useEffect(() => {
        fetchTerminalTargets().then(res => setTargets(res));
    }, []);

    const handleSelect = (id: string) => {
        setSelectedId(id);
        const t = targets.find(x => x.id === id);
        if (!t) return;

        if (t.type === "host" && t.mode === "ssh") {
            setOpenLauncher(true);
        } else {
            addTab(t);
        }
    };

    const addTab = (target: Target, credentials = {}) => {
        const instanceId = `${target.id}-${Date.now()}`;
        const newTab = { ...target, ...credentials, instanceId };
        setTabs(prev => [...prev, newTab]);
        setActiveTabId(instanceId);
    };

    const closeTab = (instanceId: string, e: React.MouseEvent) => {
        e.stopPropagation();
        const newTabs = tabs.filter(t => t.instanceId !== instanceId);
        setTabs(newTabs);
        if (activeTabId === instanceId) {
            setActiveTabId(newTabs.length > 0 ? newTabs[newTabs.length - 1].instanceId : null);
        }
    };

    const handleLaunch = () => {
        const t = targets.find(x => x.id === selectedId);
        if (t) addTab(t, form);
        setOpenLauncher(false);
    };

    return (
        <Box sx={{ width: '100%' }}>
            <Typography variant="h5" sx={{ mb: 2 }}>🖥️ Web Terminal</Typography>

            <Box sx={{ display: 'flex', gap: 2, mb: 2, alignItems: 'center' }}>
                <Select
                    size="small"
                    value={selectedId}
                    displayEmpty
                    onChange={(e) => handleSelect(e.target.value)}
                    sx={{ minWidth: 200 }}
                >
                    <MenuItem disabled value=""><em>Select Target to Open</em></MenuItem>
                    {targets.map(t => <MenuItem key={t.id} value={t.id}>{t.name}</MenuItem>)}
                </Select>
            </Box>

            {/* ✅ Tab Headers */}
            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                <Tabs value={activeTabId} onChange={(_, val) => setActiveTabId(val)} variant="scrollable">
                    {tabs.map((tab) => (
                        <Tab 
                            key={tab.instanceId} 
                            value={tab.instanceId} 
                            label={
                                <span>
                                    {tab.name} 
                                    <Button size="small" onClick={(e) => closeTab(tab.instanceId, e)} sx={{ minWidth: 'auto', ml: 1 }}>x</Button>
                                </span>
                            } 
                        />
                    ))}
                </Tabs>
            </Box>

            {/* ✅ Terminal Instances */}
            {tabs.map((tab) => (
                <Box 
                    key={tab.instanceId} 
                    role="tabpanel" 
                    sx={{ display: activeTabId === tab.instanceId ? 'block' : 'none' }}
                >
                    <WebTerminal target={tab} />
                </Box>
            ))}

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