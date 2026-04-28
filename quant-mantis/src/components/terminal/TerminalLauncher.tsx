// TerminalLauncher.tsx
import { useState } from "react";

export default function TerminalLauncher({ targets, onStart }) {
    const [selected, setSelected] = useState(null);

    const handleClick = (t) => {
        if (t.type === "host" && t.mode === "ssh") {
            const host = prompt("Host:");
            const username = prompt("Username:");
            const password = prompt("Password:");

            onStart({
                ...t,
                host,
                username,
                password
            });
        } else {
            onStart(t);
        }
    };

    return (
        <div>
            {targets.map(t => (
                <div key={t.id} onClick={() => handleClick(t)}>
                    {t.name}
                </div>
            ))}
        </div>
    );
}