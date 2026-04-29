import { useEffect, useState, useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { Box, CircularProgress, IconButton } from "@mui/material"; // 引入 IconButton
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import { fetchStockDaily } from "../../api/Client";
import { FullScreenBox } from "../misc/FullScreenBox";
import FullscreenIcon from '@mui/icons-material/Fullscreen';
import FullscreenExitIcon from '@mui/icons-material/FullscreenExit';

export const EquityChartPanel = ({ fullSection, setFullSection, viewMode }: any) => {
    const { equity, trades, selectedSymbol, setSelectedSymbol } = useBacktestResultStore();
    const [kLineData, setKLineData] = useState<any[]>([]);
    const [loading, setLoading] = useState(false);

    // K线数据加载
    useEffect(() => {
        if (viewMode === "individual" && selectedSymbol && selectedSymbol !== "average") {
            const loadData = async () => {
                setLoading(true);
                try {
                    const start = equity?.times?.[0] || '2000-01-01';
                    const end = equity?.times?.[equity?.times.length - 1] || new Date().toISOString().split('T')[0];
                    const data = await fetchStockDaily(selectedSymbol, start, end);
                    setKLineData(data);
                } catch (e) {
                    console.error("Fetch KLine Error:", e);
                } finally {
                    setLoading(false);
                }
            };
            loadData();
        }
    }, [viewMode, selectedSymbol, equity?.times]);

    // 格式化 Option (保持之前的索引对齐逻辑)
    const chartOption = useMemo(() => {
        if (!equity?.times) return {};

        // --- 个股模式 ---
        if (viewMode === "individual" && selectedSymbol && kLineData.length > 0) {
            const dates: string[] = [];
            const candleValues: number[][] = [];
            const dateToIndexMap = new Map<string, number>();

            kLineData.forEach((item, idx) => {
                const vals = item.values.map((v: any) => parseFloat(v));
                if (!isNaN(vals[0])) {
                    candleValues.push(vals);
                    const cleanDate = typeof item.date === 'string' ? item.date.split('T')[0] : String(item.date);
                    dates.push(cleanDate);
                    dateToIndexMap.set(cleanDate, idx);
                }
            });

            const markPoints = trades
                .filter(t => t.Column === selectedSymbol)
                .map(t => {
                    const date = typeof t.EntryTime === 'string' ? t.EntryTime.slice(0, 10) : String(t.EntryTime).slice(0, 10);
                    const idx = dateToIndexMap.get(date);
                    if (idx === undefined) return null;
                    const isBuy = t.Size > 0;
                    return {
                        coord: [idx, t.Price],
                        value: isBuy ? 'B' : 'S',
                        itemStyle: { color: isBuy ? '#ef5350' : '#26a69a' },
                        label: { show: true, formatter: isBuy ? 'B' : 'S', color: '#fff' }
                    };
                }).filter(Boolean);

            return {
                backgroundColor: '#141414',
                tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
                grid: { top: 40, bottom: 60, left: 50, right: 20 },
                xAxis: { type: 'category', data: dates, scale: true },
                yAxis: { type: 'value', scale: true },
                dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 5 }],
                series: [{
                    name: selectedSymbol,
                    type: 'candlestick',
                    data: candleValues,
                    itemStyle: { color: '#ef5350', color0: '#26a69a', borderColor: '#ef5350', borderColor0: '#26a69a' },
                    markPoint: { z: 10, data: markPoints }
                }]
            };
        }

        // --- 组合模式 (高亮对比) ---
        const newEquity = { "average": equity.average, ...equity.details,  };
        const series = Object.entries(newEquity).map(([symbol, arr]: any) => ({
            name: symbol,
            type: "line",
            smooth: true,
            showSymbol: false,
            emphasis: { focus: 'series' },
            lineStyle: { 
                width: selectedSymbol === symbol ? 4 : 1, 
                opacity: selectedSymbol === symbol ? 1 : (selectedSymbol ? 0.05 : 0.4) 
            },
            data: equity.times.map((t: any, i: number) => [t, arr[i]])
        }));

        return {
            backgroundColor: 'transparent',
            tooltip: { trigger: "axis" },
            xAxis: { type: "time" },
            yAxis: { type: "value", scale: true },
            series
        };
    }, [equity, trades, selectedSymbol, viewMode, kLineData]);

    // const isFull = fullSection === 'chart';

    return (
        <Box
            sx={{ 
                height: "100%", 
                width: "100%", 
                position: 'relative', 
                bgcolor: viewMode === 'individual' ? '#141414' : 'inherit',
                overflow: 'hidden'
            }}
        >
            {loading && (
                <Box sx={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)', zIndex: 100 }}>
                    <CircularProgress size={30} />
                </Box>
            )}
            
            <ReactECharts
                // 💡 只有 viewMode 变化时才销毁实例，防止点击全屏按钮时 Chart 消失
                key={viewMode} 
                option={chartOption}
                style={{ height: "100%", width: "100%" }}
                notMerge={true}
                lazyUpdate={true}
                // 💡 关键：确保在 Resize 时自动调整大小
                onChartReady={(instance) => {
                    setTimeout(() => instance.resize(), 0);
                }}
            />
        </Box>
    );
};