import SettingsIcon from "@mui/icons-material/Settings"
import {
    Box,
    Checkbox,
    IconButton,
    Tooltip,
    Typography
} from "@mui/material"
import DSLInput from "../dsl/DSLInput"
import { useState } from "react"

export default function SignalEditor({
    value,
    enabled,
    onChange,
    onToggle,
    onVisual,
    type = "signal" // 👈 signal | schedule_signal
}: any) {

    const [error, setError] = useState<string | null>(null)

    // ===== 简单前端校验（建议后面接后端）=====
    const validate = (expr: string) => {
        try {
            if (!expr) return null

            // 👉 这里先简单校验括号
            if ((expr.match(/\(/g) || []).length !== (expr.match(/\)/g) || []).length) {
                return "括号不匹配"
            }

            return null
        } catch (e: any) {
            return e.message
        }
    }

    const handleChange = (v: string) => {
        const err = validate(v)
        setError(err)
        onChange(v)
    }

    return (
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>

            {/* ===== enable ===== */}
            <Checkbox checked={enabled} onChange={onToggle} />

            {/* ===== DSL ===== */}
            <Box sx={{ flex: 1, position: "relative" }}>

                <DSLInput
                    disabled={!enabled}
                    value={value || ""}
                    onChange={handleChange}
                />

                {/* placeholder */}
                {/* placeholder (字体更大更清晰) */}
                {!value && (
                    <Typography
                        sx={{
                            position: "absolute",
                            top: 12,
                            left: 12,
                            opacity: 0.5,
                            pointerEvents: "none",
                            fontSize: "1rem",      /* 变大 */
                            lineHeight: 1.5,
                        }}
                    >
                        {type === "schedule_signal"
                            ? "e.g. cross(MA(5), MA(20))"
                            : "e.g. RSI(14) > 70"}
                    </Typography>
                )}

                {/* error */}
                {error && (
                    <Typography
                        variant="caption"
                        color="error"
                        sx={{ position: "absolute", bottom: -16, left: 4 }}
                    >
                        {error}
                    </Typography>
                )}

            </Box>

            {/* ===== Visual Editor ===== */}
            <Tooltip title="Visual Editor">
                <span>
                    <IconButton
                        onClick={onVisual}
                        disabled={!enabled}
                    >
                        <SettingsIcon />
                    </IconButton>
                </span>
            </Tooltip>

        </Box>
    )
}