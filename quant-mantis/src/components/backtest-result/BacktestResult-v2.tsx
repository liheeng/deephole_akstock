import { Card, Box } from "@mui/material";
import { useRef, useState } from "react";
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import TradesTable from "../backtest/TradesTable";
import { HorizontalSplitter, VerticalSplitter } from "../misc/Splitters"; // 假设在同文件或引入
import { EquityChartPanel } from "./EquityChartPanel";
import { StatsPanel } from "./StatsPanel";
import { FullScreenBox } from "../misc/FullScreenBox";

export default function BacktestResult_V2() {
    const { trades, selectedSymbol } = useBacktestResultStore();
    
    // 布局状态
    const [fullSection, setFullSection] = useState<string | null>(null);
    const [topHeight, setTopHeight] = useState(50);
    const [leftWidth, setLeftWidth] = useState(66);
    const [viewMode, setViewMode] = useState<"portfolio" | "individual">("portfolio");

    const containerRef = useRef<HTMLDivElement>(null);
    const isDraggingVert = useRef(false);
    const isDraggingHoriz = useRef(false);

    // 拖拽处理
    const handleMouseMove = (e: React.MouseEvent) => {
        if (!containerRef.current) return;
        const rect = containerRef.current.getBoundingClientRect();
        if (isDraggingVert.current) {
            setTopHeight(Math.max(20, Math.min(80, ((e.clientY - rect.top) / rect.height) * 100)));
        }
        if (isDraggingHoriz.current) {
            setLeftWidth(Math.max(30, Math.min(85, ((e.clientX - rect.left) / rect.width) * 100)));
        }
    };

    const handleMouseUp = () => {
        isDraggingVert.current = false;
        isDraggingHoriz.current = false;
        document.body.style.cursor = "default";
    };

    // 过滤交易数据
    const filteredTrades = selectedSymbol && selectedSymbol !== "average" 
        ? trades.filter(t => t.Column === selectedSymbol) 
        : trades;

    return (
        <Card
            ref={containerRef}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            onMouseLeave={handleMouseUp}
            sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", overflow: 'hidden' }}
        >
            {/* 上半部分 */}
            <Box sx={{ display: "flex", height: `${topHeight}%`, minHeight: 0, width: "100%" }}>
                <Box sx={{ width: `${leftWidth}%`, height: "100%" }}>
                    <EquityChartPanel 
                        fullSection={fullSection} 
                        setFullSection={setFullSection} 
                        viewMode={viewMode} 
                    />
                </Box>

                <HorizontalSplitter onMouseDown={() => { isDraggingHoriz.current = true; document.body.style.cursor = "col-resize"; }} />

                <Box sx={{ flex: 1, height: "100%", minWidth: 0 }}>
                    <StatsPanel 
                        fullSection={fullSection} 
                        setFullSection={setFullSection} 
                        viewMode={viewMode} 
                        setViewMode={setViewMode} 
                    />
                </Box>
            </Box>

            <VerticalSplitter onMouseDown={() => { isDraggingVert.current = true; document.body.style.cursor = "row-resize"; }} />

            {/* 下半部分 */}
            <Box sx={{ flex: 1, minHeight: 0 }}>
                <FullScreenBox
                    isFull={fullSection === 'trades'}
                    onToggle={() => setFullSection(fullSection === 'trades' ? null : 'trades')}
                    sx={{ height: "100%" }}
                >
                    <TradesTable trades={filteredTrades} />
                </FullScreenBox>
            </Box>
        </Card>
    );
}