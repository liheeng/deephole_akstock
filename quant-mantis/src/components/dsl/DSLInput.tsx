// components/dsl/DSLInput.tsx
import React, { useState, useRef, useMemo } from "react";
import {
    TextField,
    Popper,
    Paper,
    List,
    ListItem,
    ListItemButton,
    ListItemText,
    ClickAwayListener,
    Button,
    Stack
} from "@mui/material";
import { NodeRegistry } from "../../model/dsl_node/node_registry";

// interface DSLInputProps {
//     value: string;
//     onChange: (v: string) => void;
//     onConfirm?: (v: string) => void;
//     onCancel?: () => void;
//     placeholder?: string;
//     fullWidth?: boolean;
// }

import type { TextFieldProps } from "@mui/material";

// 👇 正确写法：Omit 掉 TextField 自带的 onChange，避免冲突
interface DSLInputProps extends Omit<TextFieldProps, 'onChange' | 'value'> {
    value: string;
    onChange: (v: string) => void;
    onConfirm?: (v: string) => void;
    onCancel?: () => void;
    placeholder?: string;
    fullWidth?: boolean;
}

export default function DSLInput({
    value,
    onChange,
    onConfirm,
    onCancel,
    placeholder,
    fullWidth = true,
    // 👇 收集所有剩下的原生 TextField 属性
    ...restProps
}: DSLInputProps) {
    const inputRef = useRef<HTMLInputElement>(null);
    const [open, setOpen] = useState(false);
    const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
    const [filtered, setFiltered] = useState<string[]>([]);
    const [selectedIndex, setSelectedIndex] = useState(0);

    // 获取所有可用的函数列表
    const allFunctions = useMemo(() => 
        Object.values(NodeRegistry.listGroups()).flatMap(g => g), 
    []);

    // 辅助函数：格式化显示名称 (Name + Parameters)
    const formatFunctionName = (name: string) => {
        const meta = NodeRegistry.getMeta(name);
        if (!meta) return name;
        const params = meta.params.map(p =>
            p.default !== null && p.default !== undefined
                ? `${p.name}=${p.default}`
                : `${p.name}`
        );
        return `${name}(${params.join(", ")})`;
    };

    // 获取当前输入光标前的单词前缀
    const getPrefix = () => {
        if (!inputRef.current) return "";
        const caret = inputRef.current.selectionStart || 0;
        const match = value.slice(0, caret).match(/([a-zA-Z0-9_]+)$/);
        return match ? match[1] : "";
    };

    const updateSuggestions = (val: string) => {
        const prefix = getPrefix();
        if (prefix.length > 0) {
            const suggestions = allFunctions.filter(fn =>
                fn.toLowerCase().startsWith(prefix.toLowerCase())
            );
            setFiltered(suggestions);
            setAnchorEl(inputRef.current?.parentElement as HTMLElement | null);
            setOpen(suggestions.length > 0);
            setSelectedIndex(0);
        } else {
            // 如果没有前缀，可以决定是否关闭，或者展示全部（类似图2下拉效果）
            setOpen(false);
        }
    };

    const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const v = e.target.value;
        onChange(v);
        // 在 state 更新后异步获取最新前缀，或直接根据新值判断
        setTimeout(() => updateSuggestions(v), 0);
    };

    const insertAtCursor = (name: string) => {
        if (!inputRef.current) return;

        const meta = NodeRegistry.getMeta(name);
        let insertText = name;

        // 构建带参数的插入文本
        if (meta) {
            const params = meta.params.map(p =>
                p.default !== null && p.default !== undefined
                    ? `${p.name}=${p.default}`
                    : `${p.name}=`
            );
            insertText = `${name}(${params.join(", ")})`;
        }

        const caret = inputRef.current.selectionStart || 0;
        const before = value.slice(0, caret).replace(/([a-zA-Z0-9_]+)$/, "");
        const after = value.slice(caret);

        const newValue = before + insertText + after;
        onChange(newValue);

        // 处理光标定位：定位到第一个参数后面
        requestAnimationFrame(() => {
            const firstParamPos = insertText.indexOf("=") + 1;
            const pos = firstParamPos > 0
                ? before.length + firstParamPos
                : before.length + insertText.length;

            inputRef.current!.setSelectionRange(pos, pos);
            inputRef.current!.focus();
        });

        setOpen(false);
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (open) {
            if (e.key === "ArrowDown") {
                e.preventDefault();
                setSelectedIndex((i) => (i + 1) % filtered.length);
            } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setSelectedIndex((i) => (i - 1 + filtered.length) % filtered.length);
            } else if (e.key === "Enter" || e.key === "Tab") {
                e.preventDefault();
                if (filtered[selectedIndex]) {
                    insertAtCursor(filtered[selectedIndex]);
                }
            } else if (e.key === "Escape") {
                setOpen(false);
            }
        } else if (e.key === "Enter") {
            onConfirm?.(value);
        }
    };

    return (
        <ClickAwayListener onClickAway={() => setOpen(false)}>
            <div style={{ width: fullWidth ? "100%" : undefined }}>
                <TextField
                    // 👇 原生属性全部透传
                    {...restProps}
                    fullWidth
                    inputRef={inputRef}
                    value={value}
                    onChange={handleChange}
                    onKeyDown={handleKeyDown}
                    onFocus={() => {
                        // 如果想在点击时就显示全部，可以调用 updateSuggestions
                        if (value.length === 0) {
                             setFiltered(allFunctions.slice(0, 10)); // 展示前10个作为参考
                             setAnchorEl(inputRef.current?.parentElement as HTMLElement | null);
                             setOpen(true);
                        }
                    }}
                    placeholder={placeholder}
                    autoComplete="off"
                />

                <Popper
                    open={open}
                    anchorEl={anchorEl}
                    placement="bottom-start"
                    sx={{ zIndex: 1300 }}
                >
                    <Paper
                        elevation={3}
                        sx={{
                            mt: 1,
                            width: 380, // 适当加宽以容纳长参数
                            maxHeight: 300,
                            overflowY: "auto",
                            borderRadius: 2
                        }}
                    >
                        <List dense>
                            {filtered.map((name, idx) => {
                                const meta = NodeRegistry.getMeta(name);
                                return (
                                    <ListItem
                                        key={name}
                                        disablePadding
                                        sx={{
                                            bgcolor: idx === selectedIndex ? "action.hover" : "transparent"
                                        }}
                                    >
                                        <ListItemButton onClick={() => insertAtCursor(name)}>
                                            <ListItemText
                                                primary={formatFunctionName(name)}
                                                secondary={meta?.group || "indicator"}
                                                sx={{
                                                    "& .MuiListItemText-primary": {
                                                        fontSize: 14,
                                                        fontWeight: 600,
                                                        fontFamily: "Monospace", // 使用等宽字体更有代码感
                                                        color: "primary.main"
                                                    },
                                                    "& .MuiListItemText-secondary": {
                                                        fontSize: 11,
                                                        marginTop: "2px",
                                                        opacity: 0.8
                                                    }
                                                }}
                                            />
                                        </ListItemButton>
                                    </ListItem>
                                );
                            })}
                        </List>
                    </Paper>
                </Popper>

                {(onConfirm || onCancel) && (
                    <Stack direction="row" spacing={1} sx={{ mt: 1, justifyContent: "flex-end" }}>
                        {onCancel && <Button size="small" onClick={onCancel}>Cancel</Button>}
                        {onConfirm && <Button size="small" variant="contained" onClick={() => onConfirm(value)}>Confirm</Button>}
                    </Stack>
                )}
            </div>
        </ClickAwayListener>
    );
}
