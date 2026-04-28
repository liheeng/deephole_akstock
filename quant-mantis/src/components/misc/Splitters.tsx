import { Box } from "@mui/material";

interface SplitterProps {
    onMouseDown: (e: React.MouseEvent) => void;
}

// 垂直分隔条 (用于左右拖拽)
export const HorizontalSplitter = ({ onMouseDown }: SplitterProps) => (
    <Box
        onMouseDown={onMouseDown}
        sx={{
            width: 6,
            mx: 0.5,
            cursor: "col-resize",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            borderRadius: 1,
            transition: "background 0.2s",
            "&:hover": {
                bgcolor: "primary.main",
                opacity: 0.5
            },
            "&::after": {
                content: '""',
                width: 2,
                height: 30,
                bgcolor: "divider",
                borderRadius: 1,
            },
        }}
    />
);

// 水平分隔条 (用于上下拖拽)
export const VerticalSplitter = ({ onMouseDown }: SplitterProps) => (
    <Box
        onMouseDown={onMouseDown}
        sx={{
            height: 6,
            my: 1,
            cursor: "row-resize",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            transition: "background 0.2s",
            "&:hover": {
                bgcolor: "primary.main",
                opacity: 0.5
            },
            "&::after": {
                content: '""',
                width: 40,
                height: 2,
                bgcolor: "divider",
                borderRadius: 1,
            },
        }}
    />
);