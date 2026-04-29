import { useEffect, useState, useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { Box, CircularProgress } from "@mui/material"; // 引入 IconButton
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import { fetchStockDaily } from "../../api/Client";

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

            // 使用 reduce 将每笔交易扁平化为 1个或2个 标注点
            const markPoints = trades
                .filter(t => t.Column === selectedSymbol)
                .reduce((points: any[], t) => {
                    
                    // ================== 1. 处理入场点 (Entry) ==================
                    const entryTime = t['Entry Timestamp'];
                    if (entryTime) {
                        const entryDate = typeof entryTime === 'string' ? entryTime.slice(0, 10) : String(entryTime).slice(0, 10);
                        const entryIdx = dateToIndexMap.get(entryDate);
                        
                        if (entryIdx !== undefined) {
                            const direction = t['Direction']; // 假设取值为 'Long' 或 'Short'
                            // 做多入场是买(B)，做空入场是卖(S)
                            const isBuyEntry = direction === 'Long'; 

                            points.push({
                                name: isBuyEntry ? 'Buy Entry' : 'Sell Entry',
                                coord: [entryIdx, t['Avg Entry Price']],
                                value: isBuyEntry ? 'B' : 'S',
                                symbol: 'pin',
                                symbolSize: 18,
                                itemStyle: { color: isBuyEntry ? '#ef5350' : '#26a69a' }, // 红买绿卖
                                label: { show: true, formatter: isBuyEntry ? 'B' : 'S', color: '#fff', fontSize: 10, fontWeight: 'bold' }
                            });
                        }
                    }

                    // ================== 2. 处理出场点 (Exit) ==================
                    // 只有当交易已完成 (可能有 Status 字段判断)，且有出场时间时才标注
                    const exitTime = t['Exit Timestamp'];
                    // 💡 修正：判断 Status 是否为 'Closed' (根据你提供的 fields，有 Status 和 PnL，说明有出场)
                    if (exitTime && t['Status'] !== 'Open') { 
                        const exitDate = typeof exitTime === 'string' ? exitTime.slice(0, 10) : String(exitTime).slice(0, 10);
                        const exitIdx = dateToIndexMap.get(exitDate);

                        if (exitIdx !== undefined) {
                            const direction = t['Direction'];
                            // 做多出场是卖(S)，做空出场是买(B)
                            const isSellExit = direction === 'Long'; 

                            points.push({
                                name: isSellExit ? 'Sell Exit' : 'Buy Exit',
                                // 💡 关键：出场点使用出场价格
                                coord: [exitIdx, t['Avg Exit Price']], 
                                value: isSellExit ? 'S' : 'B',
                                symbol: 'diamond', // 💡 用不同形状区分入场和出场，或者都用 'pin'
                                symbolSize: 18,
                                itemStyle: { 
                                    color: isSellExit ? '#26a69a' : '#ef5350', // 做多出场是平仓卖出，绿色
                                    borderColor: '#fff',
                                    borderWidth: 1
                                },
                                label: { show: true, formatter: isSellExit ? 'S' : 'B', color: '#fff', fontSize: 10 }
                            });
                        }
                    }

                    return points;
                }, []);

            // 调试打印：看看生成的点对不对
            console.log("MarkPoints:", markPoints);

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
                width: selectedSymbol === symbol ? 2 : 1, 
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