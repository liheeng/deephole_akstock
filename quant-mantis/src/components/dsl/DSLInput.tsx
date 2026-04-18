// components/dsl/DSLInput.tsx
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

interface DSLInputProps {
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
    fullWidth = true
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

// // components/dsl/DSLInput.tsx
// import React, { useState, useRef } from "react";
// import {
//     TextField,
//     Popper,
//     Paper,
//     List,
//     ListItem,
//     ListItemButton,
//     ListItemText,
//     ClickAwayListener,
//     Button,
//     Stack
// } from "@mui/material";
// import { NodeRegistry } from "../../model/dsl_node/node_registry";

// interface DSLInputProps {
//     value: string;
//     onChange: (v: string) => void;
//     onConfirm?: (v: string) => void;
//     onCancel?: () => void;
//     placeholder?: string;
//     fullWidth?: boolean;
// }

// export default function DSLInput({
//     value,
//     onChange,
//     onConfirm,
//     onCancel,
//     placeholder,
//     fullWidth = true
// }: DSLInputProps) {

//     const inputRef = useRef<HTMLInputElement>(null);

//     const [open, setOpen] = useState(false);
//     const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
//     const [filtered, setFiltered] = useState<string[]>([]);
//     const [selectedIndex, setSelectedIndex] = useState(0);

//     const allFunctions = Object.values(NodeRegistry.listGroups()).flatMap(g => g);

//     const getPrefix = () => {
//         if (!inputRef.current) return "";
//         const caret = inputRef.current.selectionStart || 0;
//         const match = value.slice(0, caret).match(/([a-zA-Z0-9_]+)$/);
//         return match ? match[1] : "";
//     };

//     const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
//         const v = e.target.value;
//         onChange(v);

//         const prefix = getPrefix();

//         if (prefix.length > 0) {
//             const suggestions = allFunctions.filter(fn =>
//                 fn.toLowerCase().startsWith(prefix.toLowerCase())
//             );
//             setFiltered(suggestions);
//             //   setAnchorEl(inputRef.current);
//             // setAnchorEl(inputRef.current?.parentElement)
//             setAnchorEl(inputRef.current?.parentElement as HTMLElement | null);
//             setOpen(suggestions.length > 0);
//             setSelectedIndex(0);
//         } else {
//             setOpen(false);
//         }
//     };

//     const insertAtCursor = (text: string) => {
//         if (!inputRef.current) return;

//         const caret = inputRef.current.selectionStart || 0;

//         const before = value.slice(0, caret).replace(/([a-zA-Z0-9_]+)$/, "");
//         const after = value.slice(caret);

//         const newValue = before + text + after;
//         onChange(newValue);

//         requestAnimationFrame(() => {
//             const firstParamPos = text.indexOf("=") + 1;

//             const pos =
//             firstParamPos > 0
//                 ? (before + text).length - text.length + firstParamPos
//                 : (before + text).length;

//             inputRef.current!.setSelectionRange(pos, pos);
//             inputRef.current!.focus();
//         });

//         setOpen(false);
//     };

//     const handleKeyDown = (e: React.KeyboardEvent) => {
//         if (open) {
//             if (e.key === "ArrowDown") {
//                 e.preventDefault();
//                 setSelectedIndex((i) => (i + 1) % filtered.length);
//             } else if (e.key === "ArrowUp") {
//                 e.preventDefault();
//                 setSelectedIndex((i) => (i - 1 + filtered.length) % filtered.length);
//             } else if (e.key === "Enter" || e.key === "Tab") {
//                 e.preventDefault();
//                 insertAtCursor(filtered[selectedIndex]);
//             } else if (e.key === "Escape") {
//                 setOpen(false);
//                 onCancel?.();
//             }
//         } else {
//             if (e.key === "Enter") {
//                 onConfirm?.(value);
//             }
//         }
//     };

//     return (
//         <ClickAwayListener onClickAway={() => setOpen(false)}>
//             <div style={{ width: fullWidth ? "100%" : undefined }}>
//                 <TextField
//                     fullWidth
//                     inputRef={inputRef}
//                     value={value}
//                     onChange={handleChange}
//                     onKeyDown={handleKeyDown}
//                     placeholder={placeholder}
//                 />

//                 <Popper
//                     open={open}
//                     anchorEl={anchorEl}
//                     placement="bottom-start"
//                     sx={{
//                         zIndex: 1300
//                     }}
//                 >
//                     <Paper
//                         sx={{
//                         mt: 1,
//                         width: 320,          // 🔥 更宽
//                         maxHeight: 260,
//                         overflowY: "auto",
//                         borderRadius: 2
//                         }}
//                     >
//                         <List dense>
//                             {filtered.map((name, idx) => {
//                                 const meta = NodeRegistry.getMeta(name);

//                                 const secondary = meta
//                                     ? `${meta.group}(${meta.params.map(p =>
//                                         p.default !== null && p.default !== undefined
//                                             ? `${p.name}=${p.default}`
//                                             : `${p.name}: ${p.type}`
//                                     ).join(", ")})`
//                                     : "";
//                                 const formatPrimary = (name: string) => {
//                                     const meta = NodeRegistry.getMeta(name);
//                                     if (!meta) return name;

//                                     const params = meta.params.map(p =>
//                                         p.default !== null && p.default !== undefined
//                                         ? `${p.name}=${p.default}`
//                                         : `${p.name}`
//                                     );

//                                     return `${name}(${params.join(", ")})`;
//                                 };

//                                 const handleSelect = (name: string) => {
//                                     const meta = NodeRegistry.getMeta(name);

//                                     let insertText = name;

//                                     if (meta) {
//                                         const params = meta.params.map(p =>
//                                         p.default !== null && p.default !== undefined
//                                             ? `${p.name}=${p.default}`
//                                             : `${p.name}=`
//                                         );

//                                         insertText = `${name}(${params.join(", ")})`;
//                                     }

//                                     insertAtCursor(insertText);
//                                 };

//                                 return (
//                                     <ListItem
//                                         key={name}
//                                         disablePadding
//                                         sx={{
//                                             bgcolor: idx === selectedIndex ? "action.hover" : "transparent"
//                                         }}
//                                         >
//                                         <ListItemButton onClick={() => handleSelect(name)}>
//                                             <ListItemText
//                                             primary={formatPrimary(name)}
//                                             secondary={meta?.group}
//                                             sx={{
//                                                 "& .MuiListItemText-primary": {
//                                                 fontSize: 15,   // 🔥 放大
//                                                 fontWeight: 500
//                                                 },
//                                                 "& .MuiListItemText-secondary": {
//                                                 fontSize: 12,
//                                                 opacity: 0.7
//                                                 }
//                                             }}
//                                             />
//                                         </ListItemButton>
//                                     </ListItem>
//                                 );
//                             })}
//                         </List>
//                     </Paper>
//                 </Popper>

//                 {(onConfirm || onCancel) && (
//                     <Stack
//                         sx={{
//                             flexDirection: "row",
//                             gap: 1,
//                             mt: 1,
//                             justifyContent: "flex-end"
//                         }}
//                     >
//                         {onCancel && (
//                             <Button
//                                 size="small"
//                                 onClick={onCancel}
//                                 sx={{ textTransform: "none" }}
//                             >
//                                 Cancel
//                             </Button>
//                         )}

//                         {onConfirm && (
//                             <Button
//                                 size="small"
//                                 variant="contained"
//                                 onClick={() => onConfirm(value)}
//                                 sx={{ textTransform: "none" }}
//                             >
//                                 Confirm
//                             </Button>
//                         )}
//                     </Stack>
//                 )}
//             </div>
//         </ClickAwayListener>
//     );
// }

// import { useState, useRef } from "react";
// import {
//   TextField,
//   Popper,
//   List,
//   ListItem,
//   ListItemButton,
//   ListItemText,
//   Paper,
//   Box,
//   Button,
//   Typography
// } from "@mui/material";
// import { NodeRegistry } from "../../model/dsl_node/node_registry";

// interface DSLInputProps {
//   value: string;
//   onChange: (v: string) => void;
//   onConfirm: (v: string) => void;
//   onCancel: () => void;
// }

// export default function DSLInput({ value, onChange, onConfirm, onCancel }: DSLInputProps) {
//   const [open, setOpen] = useState(false);
//   const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
//   const [filtered, setFiltered] = useState<string[]>([]);
//   const inputRef = useRef<HTMLInputElement>(null);

//   // 获取所有 function 名称
//   const allFunctions = Object.values(NodeRegistry.listGroups()).flatMap(group => group);

//   const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
//     const v = e.target.value;
//     onChange(v);

//     // 光标位置，显示补齐列表
//     const caret = e.target.selectionStart || 0;
//     const lastWordMatch = v.slice(0, caret).match(/([a-zA-Z0-9_]+)$/);
//     const prefix = lastWordMatch ? lastWordMatch[1] : "";

//     if (prefix) {
//       const suggestions = allFunctions.filter(name =>
//         name.toLowerCase().startsWith(prefix.toLowerCase())
//       );
//       setFiltered(suggestions);
//       setOpen(suggestions.length > 0);
//       setAnchorEl(e.target);
//     } else {
//       setOpen(false);
//     }
//   };

//   const handleSelect = (fnName: string) => {
//     if (!inputRef.current) return;

//     const el = inputRef.current;
//     const caret = el.selectionStart || 0;
//     const val = el.value;

//     // 找到最后一个单词，替换成选中函数
//     const newVal = val.slice(0, caret).replace(/([a-zA-Z0-9_]+)$/, fnName) + val.slice(caret);
//     onChange(newVal);

//     // 光标移到末尾
//     setTimeout(() => {
//       el.focus();
//       el.setSelectionRange(caret + fnName.length, caret + fnName.length);
//     }, 0);

//     setOpen(false);
//   };

//   // 参数提示
//   const paramInfo = (() => {
//     const lastWordMatch = value.match(/([a-zA-Z0-9_]+)\($/);
//     if (!lastWordMatch) return null;
//     const fn = lastWordMatch[1];
//     const meta = NodeRegistry.getMeta(fn);
//     if (!meta) return null;
//     return meta.params.map(p => `${p.name}: ${p.type} (default=${p.default})`);
//   })();

//   return (
//     <Box>
//       <TextField
//         inputRef={inputRef}
//         fullWidth
//         variant="outlined"
//         size="small"
//         value={value}
//         onChange={handleChange}
//         placeholder="Enter DSL expression..."
//       />

//       {open && anchorEl && (
//         <Popper open={open} anchorEl={anchorEl} placement="bottom-start">
//           <Paper style={{ maxHeight: 200, overflowY: "auto", width: 300 }}>
//             <List dense>
//               {filtered.map(fnName => {
//                 const meta = NodeRegistry.getMeta(fnName);
//                 return (
//                   <ListItem key={fnName} disablePadding>
//                     <ListItemButton onClick={() => handleSelect(fnName)}>
//                       <ListItemText
//                         primary={fnName}
//                         secondary={meta ? meta.params.map(p => p.name).join(", ") : ""}
//                       />
//                     </ListItemButton>
//                   </ListItem>
//                 );
//               })}
//             </List>
//           </Paper>
//         </Popper>
//       )}

//       {paramInfo && (
//         <Box sx={{ mt: 0.5, mb: 0.5 }}>
//           {paramInfo.map((p, idx) => (
//             <Typography key={idx} variant="caption" component="div">
//               {p}
//             </Typography>
//           ))}
//         </Box>
//       )}

//       <Box sx={{ display: "flex", gap: 1, mt: 1 }}>
//         <Button variant="contained" size="small" onClick={() => onConfirm(value)}>
//           Confirm
//         </Button>
//         <Button variant="outlined" size="small" onClick={onCancel}>
//           Cancel
//         </Button>
//       </Box>
//     </Box>
//   );
// }

// import {
//     Box,
//     TextField,
//     Paper,
//     List,
//     ListItemButton,
//     ListItemText,
//     Typography
// } from "@mui/material"
// import { useMemo, useRef, useState } from "react"
// import { useNodes } from "../../hooks/useNodes"
// interface Props {
//     value: string
//     onChange: (v: string) => void
//     disabled?: boolean
// }

// export default function DSLInput({ value, onChange, disabled }: Props) {
//     const nodes = useNodes()

//     const inputRef = useRef<HTMLInputElement | null>(null)

//     const [cursor, setCursor] = useState(0)
//     const [open, setOpen] = useState(false)
//     const [activeIndex, setActiveIndex] = useState(0)

//     // =========================
//     // 1️⃣ 扁平化 + 分组
//     // =========================
//     const items = useMemo(() => {
//         const res: { name: string; group: string }[] = []

//         Object.entries(nodes || {}).forEach(([group, arr]: any) => {
//             if (!Array.isArray(arr)) return
//             arr.forEach((n: any) => {
//                 if (n?.name) {
//                     res.push({
//                         name: n.name,
//                         group
//                     })
//                 }
//             })
//         })

//         return res
//     }, [nodes])

//     // =========================
//     // 2️⃣ 当前 token
//     // =========================
//     const getToken = (text: string, pos: number) => {
//         const left = text.slice(0, pos)
//         const match = left.match(/[a-zA-Z_]+$/)
//         return match ? match[0] : ""
//     }

//     // =========================
//     // 3️⃣ suggestions
//     // =========================
//     const [suggestions, setSuggestions] = useState<typeof items>([])

//     const updateSuggestions = (text: string, pos: number) => {
//         const token = getToken(text, pos)

//         if (!token) {
//             setOpen(false)
//             return
//         }

//         const list = items.filter(x =>
//             x.name.toLowerCase().startsWith(token.toLowerCase())
//         )

//         setSuggestions(list.slice(0, 30))
//         setActiveIndex(0)
//         setOpen(list.length > 0)
//     }

//     // =========================
//     // 4️⃣ 插入
//     // =========================
//     const insert = (name: string) => {
//         const el = inputRef.current
//         if (!el) return

//         const start = el.selectionStart || 0
//         const end = el.selectionEnd || 0

//         const token = getToken(value, start)

//         const before = value.slice(0, start - token.length)
//         const after = value.slice(end)

//         const text = `${name}()`
//         const next = before + text + after

//         onChange(next)

//         setTimeout(() => {
//             const pos = before.length + name.length + 1
//             el.selectionStart = el.selectionEnd = pos
//             el.focus()
//         }, 0)

//         setOpen(false)
//     }

//     // =========================
//     // 5️⃣ 输入
//     // =========================
//     const handleChange = (e: any) => {
//         const v = e.target.value
//         const pos = e.target.selectionStart

//         onChange(v)
//         setCursor(pos)
//         updateSuggestions(v, pos)
//     }

//     const handleCursor = (e: any) => {
//         const pos = e.target.selectionStart
//         setCursor(pos)
//         updateSuggestions(value, pos)
//     }

//     // =========================
//     // 6️⃣ 键盘控制（核心）
//     // =========================
//     const handleKeyDown = (e: any) => {
//         if (!open) return

//         if (e.key === "ArrowDown") {
//             e.preventDefault()
//             setActiveIndex(i => Math.min(i + 1, suggestions.length - 1))
//         }

//         if (e.key === "ArrowUp") {
//             e.preventDefault()
//             setActiveIndex(i => Math.max(i - 1, 0))
//         }

//         if (e.key === "Enter") {
//             e.preventDefault()
//             const item = suggestions[activeIndex]
//             if (item) insert(item.name)
//         }

//         if (e.key === "Escape") {
//             setOpen(false)
//         }
//     }

//     // =========================
//     // 7️⃣ 参数提示（简单版）
//     // =========================
//     const paramHint = useMemo(() => {
//         const left = value.slice(0, cursor)
//         const match = left.match(/([a-zA-Z_]+)\($/)
//         if (!match) return null

//         return match[1]
//     }, [value, cursor])

//     return (
//         <Box sx={{ position: "relative" }}>
//             <TextField
//                 fullWidth
//                 size="small"
//                 value={value}
//                 disabled={disabled}
//                 inputRef={inputRef}
//                 onChange={handleChange}
//                 onClick={handleCursor}
//                 onKeyUp={handleCursor}
//                 onKeyDown={handleKeyDown}
//                 placeholder="e.g. RSI(14) > 70"
//             />

//             {/* ================= Suggestions ================= */}
//             {open && !disabled && (
//                 <Paper
//                     sx={{
//                         position: "absolute",
//                         top: "100%",
//                         left: 0,
//                         width: 320,
//                         maxHeight: 260,
//                         overflow: "auto",
//                         zIndex: 20
//                     }}
//                 >
//                     <List dense>
//                         {suggestions.map((s, i) => (
//                             <ListItemButton
//                                 key={i}
//                                 selected={i === activeIndex}
//                                 onMouseDown={() => insert(s.name)}
//                             >
//                                 <ListItemText
//                                     primary={s.name}
//                                     secondary={s.group}
//                                 />
//                             </ListItemButton>
//                         ))}
//                     </List>
//                 </Paper>
//             )}

//             {/* ================= Param Hint ================= */}
//             {paramHint && (
//                 <Paper
//                     sx={{
//                         position: "absolute",
//                         top: -30,
//                         left: 0,
//                         px: 1,
//                         py: 0.5,
//                         fontSize: 12
//                     }}
//                 >
//                     <Typography variant="caption">
//                         {paramHint}(param1, param2)
//                     </Typography>
//                 </Paper>
//             )}
//         </Box>
//     )
// }