import { useEffect, useState, useMemo, useRef } from "react";
import ReactECharts from "echarts-for-react";
import { Box, CircularProgress } from "@mui/material"; // 引入 IconButton
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import { fetchStockDaily } from "../../api/Client";

export const EquityChartPanel = ({ fullSection, setFullSection, viewMode }: any) => {
    const { equity, trades, selectedSymbol, setSelectedSymbol, activeTradeId } = useBacktestResultStore();
    const chartRef = useRef<any>(null);
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

    // 💡 监听 activeTradeId 变化并触发 ECharts 动作
    // 💡 监听 activeTradeId 变化
    useEffect(() => {
        if (!activeTradeId || !chartRef.current) return;

        const echartsInstance = chartRef.current.getEchartsInstance();

        // 1. 取消旧高亮
        echartsInstance.dispatchAction({
            type: 'downplay',
            seriesIndex: 0
        });

        // 2. 触发新高亮 (入场和出场同时)
        const targetNames = [`${activeTradeId}_entry`, `${activeTradeId}_exit`];

        echartsInstance.dispatchAction({
            type: 'highlight',
            seriesIndex: 0,
            name: targetNames
        });

        // 3. 自动定位视图 (DataZoom)
        // 找到对应的交易数据
        const targetTrade = trades.find(t => t['Exit Trade Id'] === activeTradeId);
        if (targetTrade) {
            const entryDate = targetTrade['Entry Timestamp'].slice(0, 10);
            const option = echartsInstance.getOption();
            const dates = option.xAxis[0].data;
            const entryIdx = dates.indexOf(entryDate);

            if (entryIdx !== -1) {
                // 将视图中心移动到该点，显示前后 30 根蜡烛
                echartsInstance.dispatchAction({
                    type: 'dataZoom',
                    startValue: Math.max(0, entryIdx - 30),
                    endValue: Math.min(dates.length - 1, entryIdx + 30)
                });
            }
        }

        // 4. 弹出 Tooltip
        // 注意：showTip 最好针对具体的 markPoint 坐标触发
        echartsInstance.dispatchAction({
            type: 'showTip',
            seriesIndex: 0,
            name: `${activeTradeId}_entry` // 默认弹入场点的提示
        });

    }, [activeTradeId, trades]);

    const renderMarkPointTooltip = (d: any) => {
        // 根据类型（入场/出场）决定标题颜色
        const titleColor = d.type === 'Entry' ? (d.action.includes('买入') ? '#ef5350' : '#26a69a') : '#fff';

        let html = `
        <div style="padding: 3px 6px;">
            <div style="color: ${titleColor}; font-weight: bold; font-size: 14px; border-bottom: 1px solid #555; padding-bottom: 4px; margin-bottom: 6px;">
                ${d.action} (${d.direction})
            </div>
            <div style="display: flex; justify-content: space-between; gap: 15px; margin-bottom: 3px;">
                <span style="color: #aaa;">价格:</span>
                <span style="color: #fff; font-weight: bold;">${d.price}</span>
            </div>
            <div style="display: flex; justify-content: space-between; gap: 15px; margin-bottom: 3px;">
                <span style="color: #aaa;">数量:</span>
                <span style="color: #fff;">${d.quantity}</span>
            </div>
            <div style="display: flex; justify-content: space-between; gap: 15px; margin-bottom: 3px;">
                <span style="color: #aaa;">时间:</span>
                <span style="color: #fff;">${d.time.slice(0, 16)}</span>
            </div>
    `;

        // 如果是出场点，增加 PnL 显示
        if (d.type === 'Exit') {
            const pnlColor = parseFloat(d.pnl) >= 0 ? '#ef5350' : '#26a69a';
            html += `
            <div style="border-top: 1px solid #555; margin-top: 6px; padding-top: 4px;">
                <div style="display: flex; justify-content: space-between; gap: 15px; margin-bottom: 3px;">
                    <span style="color: #aaa;">PnL:</span>
                    <span style="color: ${pnlColor}; font-weight: bold;">${d.pnl} (${d.return})</span>
                </div>
            </div>
        `;
        } else {
            // 入场点显示 Status
            html += `
            <div style="display: flex; justify-content: space-between; gap: 15px; margin-bottom: 3px;">
                <span style="color: #aaa;">状态:</span>
                <span style="color: #eee;">${d.status}</span>
            </div>
        `;
        }

        html += '</div>';
        return html;
    };

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
            // 使用 reduce 将每笔交易扁平化为 1个或2个 标注点
            const markPoints = trades
                .filter(t => t.Column === selectedSymbol)
                .reduce((points: any[], t) => {
                    // 定义一些通用数据，用于 Tooltip 显示
                    const directionStr = t['Direction'] === 'Long' ? '做多' : '做空';
                    const size = Math.abs(t['Size']);
                    const pnl = t['PnL'] ? t['PnL'].toFixed(2) : '--';
                    const returnRate = t['Return'] ? (t['Return'] * 100).toFixed(2) + '%' : '--';

                    // ================== 1. 处理入场点 (Entry) ==================
                    const entryTime = t['Entry Timestamp'];
                    if (entryTime) {
                        const entryDate = entryTime.slice(0, 10);
                        const entryIdx = dateToIndexMap.get(entryDate);

                        if (entryIdx !== undefined) {
                            const isBuyEntry = t['Direction'] === 'Long';
                            const tradeId = t['Exit Trade Id']; // 确保这里有 ID
                            points.push({
                                // ECharts 原生属性
                                // name: isBuyEntry ? '买入开仓' : '卖出开仓',
                                name: `${tradeId}_entry`, // 区分入场出场
                                coord: [entryIdx, t['Avg Entry Price']],
                                value: isBuyEntry ? 'B' : 'S',
                                symbol: 'pin',
                                symbolSize: 20,
                                itemStyle: { color: isBuyEntry ? '#ef5350' : '#26a69a' },
                                label: { show: true, formatter: isBuyEntry ? 'B' : 'S', color: '#fff', fontWeight: 'bold' },

                                emphasis: {
                                    itemStyle: {
                                        borderWidth: 3,
                                        borderColor: '#fff',
                                        shadowBlur: 15,
                                        shadowColor: '#fff',
                                        symbolSize: 35
                                    },
                                    label: {
                                        fontSize: 14,
                                        fontWeight: 'bold'
                                    },
                                    scale: true
                                },
                                // 💡 关键 1: 注入自定义 Detail 数据供 Tooltip 使用
                                // 这些属性会被 ECharts 放在 params.data 内部
                                detail: {
                                    type: 'Entry',
                                    action: isBuyEntry ? '买入 B' : '卖出 S',
                                    time: entryTime,
                                    price: t['Avg Entry Price'].toFixed(3),
                                    direction: directionStr,
                                    quantity: size,
                                    status: t['Status']
                                }
                            });
                        }
                    }

                    // ================== 2. 处理出场点 (Exit) ==================
                    const exitTime = t['Exit Timestamp'];
                    if (exitTime && t['Status'] !== 'Open') {
                        const exitDate = exitTime.slice(0, 10);
                        const exitIdx = dateToIndexMap.get(exitDate);

                        if (exitIdx !== undefined) {
                            const isSellExit = t['Direction'] === 'Long';
                            const tradeId = t['Exit Trade Id']; // 确保这里有 ID
                            points.push({
                                // name: isSellExit ? '卖出平仓' : '买入平仓',
                                name: `${tradeId}_exit`,
                                coord: [exitIdx, t['Avg Exit Price']],
                                value: isSellExit ? 'S' : 'B',
                                symbol: 'diamond',
                                symbolSize: 18,
                                itemStyle: { color: isSellExit ? '#26a69a' : '#ef5350', borderColor: '#fff', borderWidth: 1 },
                                label: { show: true, formatter: isSellExit ? 'S' : 'B', color: '#fff', fontSize: 10 },

                                emphasis: {
                                    itemStyle: {
                                        borderWidth: 3,
                                        borderColor: '#fff',
                                        shadowBlur: 15,
                                        shadowColor: '#fff',
                                        symbolSize: 35
                                    },
                                    label: {
                                        fontSize: 14,
                                        fontWeight: 'bold'
                                    },
                                    scale: true
                                },
                                // 💡 关键 2: 注入出场 Detail 数据
                                detail: {
                                    type: 'Exit',
                                    action: isSellExit ? '卖出 S' : '买入 B',
                                    time: exitTime,
                                    price: t['Avg Exit Price'].toFixed(3),
                                    direction: directionStr,
                                    quantity: size,
                                    pnl: pnl,
                                    return: returnRate
                                }
                            });
                        }
                    }
                    return points;
                }, []);

            // 调试打印：看看生成的点对不对
            console.log("MarkPoints:", markPoints);

            return {
                backgroundColor: '#141414',
                tooltip: {
                    trigger: 'axis', // 1. 必须用 axis 保证 K 线准星和数据平滑显示
                    axisPointer: {
                        type: 'cross',
                        label: { backgroundColor: '#555' }
                    },
                    backgroundColor: 'rgba(30, 30, 30, 0.9)',
                    borderColor: '#555',
                    confine: true,
                    textStyle: { color: '#eee' },

                    // 2. 核心逻辑：在数组中探测数据
                    formatter: function (params: any) {
                        if (!params || params.length === 0) return '';

                        // --- A. 优先检测是否“踩”到了买卖点 ---
                        // 在 axis 模式下，如果鼠标碰巧悬停在 markPoint 上，
                        // 某些版本的 ECharts 会将 markPoint 的数据塞进对应 series 的对象中
                        // 我们通过探测我们自定义的 'detail' 字段来抓取它
                        let markPointHtml = '';

                        // 遍历当前轴上的所有 series 数据
                        for (let p of params) {
                            // 探测点位：ECharts 在 axis 触发时，如果是 markPoint 上的点，
                            // 数据可能存在于特殊的交互上下文中。
                            // 如果直接探测不到，我们还有一个保底方案。
                            if (p.data && p.data.detail) {
                                markPointHtml = renderMarkPointTooltip(p.data.detail);
                                break;
                            }
                        }

                        if (markPointHtml) return markPointHtml;

                        // --- B. 如果没踩到买卖点，显示常规 K 线数据 ---
                        const kData = params.find((p: any) => p.seriesType === 'candlestick');
                        if (kData) {
                            const d = kData.data; // [index, open, close, low, high]
                            const isUp = d[2] >= d[1];
                            const color = isUp ? '#ef5350' : '#26a69a';

                            return `
                    <div style="font-size: 12px; border-bottom: 1px solid #555; padding-bottom: 4px; margin-bottom: 4px;">
                        ${kData.name}
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
                        <span>开: <span style="color: #fff">${d[1]}</span></span>
                        <span>收: <span style="color: ${color}">${d[2]}</span></span>
                        <span>低: <span style="color: #fff">${d[3]}</span></span>
                        <span>高: <span style="color: #fff">${d[4]}</span></span>
                    </div>
                `;
                        }
                        return '';
                    }
                },
                grid: { top: 40, bottom: 60, left: 50, right: 20 },
                xAxis: { type: 'category', data: dates, scale: true },
                yAxis: { type: 'value', scale: true },
                dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 5 }],
                series: [{
                    name: selectedSymbol,
                    type: 'candlestick',
                    data: candleValues,
                    itemStyle: { color: '#ef5350', color0: '#26a69a', borderColor: '#ef5350', borderColor0: '#26a69a' },
                    // 🚨 强制 K线 series 不触发 axis tooltip，
                    // 这样鼠标移到 pin 上时，'item' 触发器才能起作用
                    // tooltip: { trigger: 'item' },

                    // --- 2. 这里的 tooltip 配置只影响 markPoint ---
                    markPoint: {
                        z: 10,
                        data: markPoints,
                        // 💡 强制 markPoint 响应鼠标，但不拦截全局 tooltip
                        tooltip: {
                            trigger: 'item',
                            formatter: (p: any) => renderMarkPointTooltip(p.data.detail)
                        }
                    }
                }]
            };
        }

        // --- 组合模式 (高亮对比) ---
        const newEquity = { "average": equity.average, ...equity.details, };
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
                ref={chartRef} // 💡 必须绑定 ref
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