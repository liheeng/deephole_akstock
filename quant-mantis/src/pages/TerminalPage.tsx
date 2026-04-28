import {
    Box,
    MenuItem,
    Select,
    Typography,
    Dialog,
    DialogTitle,
    DialogContent,
    TextField,
    DialogActions,
    Button
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

export default function TerminalPage() {
    const [targets, setTargets] = useState<Target[]>([]);
    const [selectedId, setSelectedId] = useState("");

    const [activeTarget, setActiveTarget] = useState<any>(null);

    // 👇 launcher form
    const [openLauncher, setOpenLauncher] = useState(false);
    const [form, setForm] = useState({
        host: "",
        username: "",
        password: ""
    });

    useEffect(() => {
        fetchTerminalTargets().then(res => {
            setTargets(res);
            if (res.length) setSelectedId(res[0].id);
        });
    }, []);

    const handleSelect = (id: string) => {
        setSelectedId(id);

        const t = targets.find(x => x.id === id);
        if (!t) return;

        // 🔥 host（ssh）走弹窗
        if (t.type === "host" && t.mode === "ssh") {
            setOpenLauncher(true);
            return;
        }

        // 🔥 其他直接启动
        setActiveTarget(t);
    };

    const handleLaunch = () => {
        const t = targets.find(x => x.id === selectedId);
        if (!t) return;

        setActiveTarget({
            ...t,
            ...form
        });

        setOpenLauncher(false);
    };

    return (
        <Box>
            <Typography variant="h5" sx={{ mb: 2 }}>
                🖥️ Web Terminal
            </Typography>

            <Select
                size="small"
                value={selectedId}
                onChange={(e) => handleSelect(e.target.value)}
                sx={{ mb: 2, minWidth: 260 }}
            >
                {targets.map(t => (
                    <MenuItem key={t.id} value={t.id}>
                        {t.name}
                    </MenuItem>
                ))}
            </Select>
            
            
            {/* ✅ terminal */}
            {activeTarget && (
                <WebTerminal target={activeTarget}/>
            )}

            {/* ✅ SSH launcher */}
            <Dialog open={openLauncher} onClose={() => setOpenLauncher(false)}>
                <DialogTitle>Connect via SSH</DialogTitle>

                <DialogContent sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 1 }}>
                    <TextField
                        label="Host"
                        value={form.host}
                        onChange={(e) =>
                            setForm({ ...form, host: e.target.value })
                        }
                    />

                    <TextField
                        label="Username"
                        value={form.username}
                        onChange={(e) =>
                            setForm({ ...form, username: e.target.value })
                        }
                    />

                    <TextField
                        label="Password"
                        type="password"
                        value={form.password}
                        onChange={(e) =>
                            setForm({ ...form, password: e.target.value })
                        }
                    />
                </DialogContent>

                <DialogActions>
                    <Button onClick={() => setOpenLauncher(false)}>
                        Cancel
                    </Button>
                    <Button variant="contained" onClick={handleLaunch}>
                        Connect
                    </Button>
                </DialogActions>
            </Dialog>
        </Box>
    );
}