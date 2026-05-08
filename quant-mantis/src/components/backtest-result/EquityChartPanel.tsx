import { useEffect, useState, useMemo, useRef } from "react";
import ReactECharts from "echarts-for-react";
import { Box, CircularProgress } from "@mui/material"; // 引入 IconButton
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import { fetchStockDaily } from "../../api/Client";

export const EquityChartPanel = ({ viewMode }: any) => {
    const { equity, trades, selectedSymbol, activeTradeId, setActiveTradeId } = useBacktestResultStore();
    const chartRef = useRef<any>(null);
    const [kLineData, setKLineData] = useState<any[]>([]);
    const [loading, setLoading] = useState(false);

    // K线数据加载
    useEffect(() => {
        if (viewMode === "individual" && selectedSymbol && selectedSymbol !== "average") {
            // clear active trade id while symbol is changed.
            setActiveTradeId(null)

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
    useEffect(() => {
        // 如果没有选中项，不做任何事（或者可以发送 downplay 取消 Tooltip）
        if (!activeTradeId || !chartRef.current) return;

        const echartsInstance = chartRef.current.getEchartsInstance();

        // 💡 这里我们只负责弹出 Tooltip，样式的改变通过 useMemo 的 Update 实现
        // 弹出 Tooltip 时，默认点到入场点
        echartsInstance.dispatchAction({
            type: 'showTip',
            seriesIndex: 0,
            name: [`${activeTradeId}_entry`, `${activeTradeId}_exit`]
        });

    }, [activeTradeId]);

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
            const volumes: any[] = []; // 💡 存储成交量数据
            const dateToIndexMap = new Map<string, number>();

            kLineData.forEach((item, idx) => {
                const vals = item.values.map((v: any) => parseFloat(v));
                if (!isNaN(vals[0])) {
                    candleValues.push(vals); // [open, close, low, high]

                    // 💡 成交量逻辑：[索引, 数值, 颜色标记]
                    const isUp = vals[1] >= vals[0];
                    volumes.push([idx, item.volume, isUp ? 1 : -1]);

                    const cleanDate = typeof item.date === 'string' ? item.date.split('T')[0] : String(item.date);
                    dates.push(cleanDate);
                    dateToIndexMap.set(cleanDate, idx);
                }
            });

            const selectedTrades = trades.filter(t => t.Column === selectedSymbol);
            const markPoints = selectedTrades.reduce((points: any[], t) => {
                const directionStr = t['Direction'] === 'Long' ? '做多' : '做空';
                const size = Math.abs(t['Size']);
                const pnl = t['PnL'] ? t['PnL'].toFixed(2) : '--';
                const returnRate = t['Return'] ? (t['Return'] * 100).toFixed(2) + '%' : '--';
                const tradeId = t['Exit Trade Id'];
                const isSelected = activeTradeId === tradeId;

                // 1. 入场点
                const entryTime = t['Entry Timestamp'];
                if (entryTime) {
                    const entryDate = entryTime.slice(0, 10);
                    const entryIdx = dateToIndexMap.get(entryDate);
                    if (entryIdx !== undefined) {
                        const isBuyEntry = t['Direction'] === 'Long';
                        points.push({
                            name: `${tradeId}_entry`,
                            coord: [entryIdx, t['Avg Entry Price']],
                            value: isBuyEntry ? 'B' : 'S',
                            symbol: 'circle',
                            symbolSize: isSelected ? 26 : 20,
                            // 核心：白底 + 对应颜色边框
                            itemStyle: {
                                backgroundColor: isSelected ? '#fababa' : '#fff', // 白色背景（ TradingView 标配）
                                borderColor: isSelected ? '#860101' : (isBuyEntry ? '#ef5350' : '#26a69a'), // 边框：红/绿
                                borderWidth: 2,
                                color: isSelected ? '#fababa' : '#fff' // 图标颜色
                            },
                            // 文字标签：红/绿 颜色，白底风格
                            label: {
                                show: true,
                                formatter: isBuyEntry ? 'B' : 'S',
                                fontSize: isSelected ? 12: 10,
                                color: isSelected ? '#891d02' : (isBuyEntry ? '#ef5350' : '#26a69a'), // 文字颜色
                                backgroundColor: isSelected ? '#fababa' : '#fff', // 文字背景白色
                                borderColor: isSelected ? '#fababa' : '#fff',
                                borderWidth: 1,
                                padding: [2, 4]
                            },
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

                // 2. 出场点
                const exitTime = t['Exit Timestamp'];
                if (exitTime && t['Status'] !== 'Open') {
                    const exitDate = exitTime.slice(0, 10);
                    const exitIdx = dateToIndexMap.get(exitDate);
                    if (exitIdx !== undefined) {
                        const isSellExit = t['Direction'] === 'Long';
                        points.push({
                            name: `${tradeId}_exit`,
                            coord: [exitIdx, t['Avg Exit Price']],
                            value: isSellExit ? 'S' : 'B',
                            symbol: 'rect',
                            symbolSize: isSelected ? 24 : 16,
                            // 样式：白底 + 彩色边框
                            itemStyle: {
                                backgroundColor: isSelected ? '#fababa' : '#fff',
                                borderColor: isSelected ? '#005149' : (isSellExit ? '#26a69a' : '#ef5350'),
                                borderWidth: 2,
                                color: isSelected ? '#fababa' : '#fff'
                            },
                            // 标签：白底 + 对应颜色文字/边框
                            label: {
                                show: true,
                                formatter: isSellExit ? 'S' : 'B',
                                fontSize: isSelected ? 12 : 10,
                                color: isSelected ? '#005149' : (isSellExit ? '#26a69a' : '#ef5350'),
                                backgroundColor: isSelected ? '#fababa' : '#fff',
                                borderColor: isSelected ? '#fababa' : '#fff',
                                borderWidth: 1,
                                padding: [2, 4]
                            },
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

            // --- 缩放定位逻辑 ---
            // let zoomConfig: any[] = [{ type: 'inside' }, { type: 'slider', bottom: 5 }];
            let zoomConfig: any[] = [
                { type: 'inside', xAxisIndex: [0, 1], start: 80, end: 100 },
                { type: 'slider', xAxisIndex: [0, 1], bottom: 5, start: 80, end: 100 }
            ];
            let firstTrade: any
            let lastTrade: any
            if (activeTradeId) {
                firstTrade = selectedTrades.find(t => t['Exit Trade Id'] === activeTradeId);
                lastTrade = firstTrade
            } else {
                firstTrade = selectedTrades.at(0)
                lastTrade = selectedTrades.at(-1)
            }
            if (firstTrade) {
                const entryDate = firstTrade['Entry Timestamp'].slice(0, 10);
                const entryIdx = dateToIndexMap.get(entryDate);
                const exitDate = lastTrade['Exit Timestamp'].slice(0, 10);
                const exitIdx = dateToIndexMap.get(exitDate);
                if (entryIdx !== undefined) {
                    zoomConfig = [
                        { type: 'inside', xAxisIndex: [0, 1], startValue: Math.max(0, entryIdx - 30), endValue: Math.min(dates.length - 1, exitIdx ? (exitIdx + 30) : (entryIdx + 60)) },
                        { type: 'slider', xAxisIndex: [0, 1], brushSelect: true, bottom: 5, startValue: Math.max(0, entryIdx - 30), endValue: Math.min(dates.length - 1, exitIdx ? (exitIdx + 30) : (entryIdx + 60)) }
                    ];
                }
            }

            return {
                backgroundColor: '#141414',
                // 💡 联动上下两个网格的准星
                axisPointer: { link: [{ xAxisIndex: 'all' }] },
                toolbox: {
                    right: 20,
                    top: 10,
                    // 图标默认样式
                    iconStyle: {
                        borderColor: '#888', // 默认灰色边框
                        borderWidth: 1
                    },
                    // 💡 关键：修改悬停时的样式
                    emphasis: {
                        iconStyle: {
                            borderColor: '#f8f894', // 悬停时变为亮黄色（与你选中的 MarkPoint 一致）
                            borderWidth: 2,
                            shadowBlur: 10,
                            shadowColor: 'rgba(248, 248, 148, 0.5)'
                        }
                    },
                    feature: {
                        // 💡 自定义按钮：清除选中的交易（重置视角）
                        myClearTrade: {
                            show: activeTradeId ? true : false,
                            title: '清除选中交易',
                            // 💡 修正后的居中路径
                            icon: 'path://M12,2C6.47,2,2,6.47,2,12s4.47,10,10,10,10-4.47,10-10S17.53,2,12,2Zm5,13.59L15.59,17,12,13.41,8.41,17,7,15.59,10.59,12,7,8.41,8.41,7,12,10.59,15.59,7,17,8.41,13.41,12,17,15.59Z',
                            onclick: () => {
                                setActiveTradeId(null);
                            }
                        },
                        // 💡 自定义工具按钮
                        myLegendSwitch: {
                            show: true,
                            title: '显示/隐藏图例',
                            // 这里你可以找一个类似列表或眼睛的 SVG 路径
                            icon: 'path://M4.1,27.3V8.1h21.9v19.2H4.1z M5.6,25.8h18.9V9.6H5.6V25.8z M8.6,12.6h12.9v1.5H8.6V12.6z M8.6,17.1h12.9v1.5H8.6V17.1z M8.6,21.6h12.9v1.5H8.6V21.6z',
                            onclick: () => {
                                if (!chartRef.current) return;
                                const echartsInstance = chartRef.current.getEchartsInstance();
                                const currentOption = echartsInstance.getOption();

                                // 💡 切换 legend 的 show 属性
                                const isLegendVisible = currentOption.legend[0].show;

                                echartsInstance.setOption({
                                    legend: {
                                        show: !isLegendVisible
                                    }
                                });
                            }
                        },
                        dataZoom: {
                            yAxisIndex: 'none',
                            title: {
                                zoom: '区域缩放',
                                back: '撤销缩放' // 💡 这就是你想要的 Undo 功能
                            }
                        },
                        restore: { title: '重置视图' },
                        saveAsImage: { title: '保存图片' }
                    }
                },
                // 记得初始配置中也要有 legend 的定义
                legend: {
                    show: true, // 初始显示
                    textStyle: { color: '#ccc' },
                    top: 10,
                    left: 'center'
                },
                tooltip: {
                    trigger: 'axis',
                    axisPointer: { type: 'cross' },
                    backgroundColor: 'rgba(30, 30, 30, 0.9)',
                    confine: true,
                    formatter: function (params: any) {
                        // 1. 如果鼠标在 markPoint 上，不渲染 axis 内容（交给 markPoint 内部 formatter）
                        // 但由于 ECharts 机制，这里我们可以做一个合并处理
                        const kData = params.find((p: any) => p.seriesType === 'candlestick');
                        const vData = params.find((p: any) => p.seriesName === 'Volume');

                        let html = `<div style="font-size: 12px; border-bottom: 1px solid #555; padding-bottom: 4px; margin-bottom: 4px; color: #888;">${params[0].name}</div>`;

                        if (kData) {
                            const d = kData.data; // [index, open, close, low, high]
                            const color = d[2] >= d[1] ? '#ef5350' : '#26a69a';
                            html += `<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
                                    <span>开: <span style="color: #fff">${d[1]}</span></span>
                                    <span>收: <span style="color: ${color}">${d[2]}</span></span>
                                    <span>低: <span style="color: #fff">${d[3]}</span></span>
                                    <span>高: <span style="color: #fff">${d[4]}</span></span>
                                 </div>`;
                        }
                        if (vData) {
                            // vData.data 是 [idx, volume, colorFlag]
                            const vol = vData.data[1] || vData.data.value[1];
                            html += `<div style="margin-top:4px; border-top:1px dashed #444; padding-top:4px;">成交量: <span style="color:#fff">${vol.toLocaleString()}</span></div>`;
                        }
                        return html;
                    }
                },
                grid: [
                    { left: 50, right: 20, top: 40, height: '72%' }, // K线区
                    { left: 50, right: 20, top: '78%', height: '15%' } // 成交量区
                ],
                xAxis: [
                    { type: 'category', data: dates, scale: true, gridIndex: 0, boundaryGap: true },
                    { type: 'category', data: dates, scale: true, gridIndex: 1, axisLabel: { show: false }, axisTick: { show: false } }
                ],
                yAxis: [
                    { type: 'value', scale: true, gridIndex: 0, splitLine: { lineStyle: { color: '#222' } } },
                    { type: 'value', scale: true, gridIndex: 1, splitNumber: 2, axisLabel: { show: false }, splitLine: { show: false } }
                ],
                dataZoom: zoomConfig,
                series: [
                    {
                        name: selectedSymbol,
                        type: 'candlestick',
                        xAxisIndex: 0,
                        yAxisIndex: 0,
                        data: candleValues,
                        itemStyle: { color: '#ef5350', color0: '#26a69a', borderColor: '#ef5350', borderColor0: '#26a69a' },
                        markPoint: {
                            z: 10,
                            data: markPoints,
                            tooltip: {
                                trigger: 'item', // 💡 只有移入标记点才触发这个
                                formatter: (p: any) => renderMarkPointTooltip(p.data.detail)
                            }
                        }
                    },
                    {
                        name: 'Volume',
                        type: 'bar',
                        xAxisIndex: 1,
                        yAxisIndex: 1,
                        data: volumes.map(v => ({
                            value: v, // [idx, vol, colorFlag]
                            itemStyle: { color: v[2] === 1 ? '#ef5350' : '#26a69a', opacity: 0.8 }
                        }))
                    }
                ]
            };
        }

        // --- 组合模式 ---
        // (保持原样...)
        const newEquity = { "average": equity.average, ...equity.details, };
        return {
            backgroundColor: 'transparent',
            toolbox: {
                right: 20,
                top: 10,
                // 图标默认样式
                iconStyle: {
                    borderColor: '#888', // 默认灰色边框
                    borderWidth: 1
                },
                // 💡 关键：修改悬停时的样式
                emphasis: {
                    iconStyle: {
                        borderColor: '#f8f894', // 悬停时变为亮黄色（与你选中的 MarkPoint 一致）
                        borderWidth: 2,
                        shadowBlur: 10,
                        shadowColor: 'rgba(248, 248, 148, 0.5)'
                    }
                },
                feature: {
                    // 💡 自定义工具按钮
                    myLegendSwitch: {
                        show: true,
                        title: '显示/隐藏图例',
                        // 这里你可以找一个类似列表或眼睛的 SVG 路径
                        icon: 'path://M4.1,27.3V8.1h21.9v19.2H4.1z M5.6,25.8h18.9V9.6H5.6V25.8z M8.6,12.6h12.9v1.5H8.6V12.6z M8.6,17.1h12.9v1.5H8.6V17.1z M8.6,21.6h12.9v1.5H8.6V21.6z',
                        onclick: () => {
                            if (!chartRef.current) return;
                            const echartsInstance = chartRef.current.getEchartsInstance();
                            const currentOption = echartsInstance.getOption();

                            // 💡 切换 legend 的 show 属性
                            const isLegendVisible = currentOption.legend[0].show;

                            echartsInstance.setOption({
                                legend: {
                                    show: !isLegendVisible
                                }
                            });
                        }
                    },
                    dataZoom: {
                        yAxisIndex: 'none',
                        title: {
                            zoom: '区域缩放',
                            back: '撤销缩放' // 💡 这就是你想要的 Undo 功能
                        }
                    },
                    restore: { title: '重置视图' },
                    saveAsImage: { title: '保存图片' }
                }
            },
            tooltip: { trigger: "axis" },
            xAxis: { type: "time" },
            yAxis: { type: "value", scale: true },
            // 记得初始配置中也要有 legend 的定义
            legend: {
                show: true, // 初始显示
                textStyle: { color: '#ccc' },
                top: 10,
                left: 'center'
            },
            series: Object.entries(newEquity).map(([symbol, arr]: any) => {
                // 💡 定义判断条件：是否是当前选中的 Symbol 或默认的 Average
                const isHighlighted = (selectedSymbol === symbol || (selectedSymbol == null && symbol === 'average'));

                return {
                    name: symbol,
                    type: "line",
                    smooth: true,
                    showSymbol: false,
                    // z 轴：确保高亮的线在最上层，不被遮挡
                    z: isHighlighted ? 10 : 1,
                    emphasis: {
                        focus: 'series',
                        lineStyle: { width: isHighlighted ? 3 : 2 } // 鼠标悬停时进一步加粗
                    },
                    lineStyle: {
                        width: isHighlighted ? 3 : 1,
                        // 💡 亮度提升逻辑：
                        // 高亮线用 1 (不透明)，非高亮线用 0.2 或 0.3 (半透明/变暗)
                        opacity: isHighlighted ? 1 : 0.3,

                        // 💡 增加发光效果 (仅针对高亮线)
                        shadowBlur: isHighlighted ? 1 : 0,
                        shadowColor: isHighlighted ? 'inherit' : 'transparent', // 继承线的颜色作为发光色
                    },
                    data: equity.times.map((t: any, i: number) => [t, arr[i]])
                };
            })
        };
    }, [equity, trades, selectedSymbol, viewMode, kLineData, activeTradeId]);

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