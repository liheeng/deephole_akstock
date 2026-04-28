import { Box, MenuItem, Select, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import WebTerminal from "../components/WebTerminal";
import { fetchTerminalTargets } from "../api/Client"

type Target = {
    id: string;
    name: string;
    type: string;
};

export default function TerminalPage() {
    const [targets, setTargets] = useState<Target[]>([]);
    const [target, setTarget] = useState("");

    useEffect(() => {
        fetchTerminalTargets().then(res => {
            // originalSnapshot
            setTargets(res);
        if (res.length)
            setTarget(res[0].id);
        })
    }, []);

    return (
        <Box>
            <Typography variant="h5" sx={{mb:2}}>
                🖥️ Web Terminal
            </Typography>

            <Select
                size="small"
                value={target}
                onChange={(e) => setTarget(e.target.value)}
                sx={{ mb: 2, minWidth: 260 }}
            >
                {targets.map(t => (
                    <MenuItem key={t.id} value={t.id}>
                        {t.name}
                    </MenuItem>
                ))}
            </Select>

            {target && <WebTerminal target={target} />}
        </Box>
    );
}