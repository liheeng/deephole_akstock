import ReactECharts from "echarts-for-react";
import { useBacktestResultStore } from "../../store/backtest/backtestresult.store";
import { useMemo } from "react";
import { FullScreenBox } from "../misc/FullScreenBox";

export const EquityChartPanel = ({ fullSection, setFullSection, viewMode }: any) => {
    const { equity, selectedSymbol, setSelectedSymbol } = useBacktestResultStore();
    
    const replaceEmji = (s: string) => s.replace(/⭐|🚀|/g, "").trim();
    const formatDate = (ts: any) => {
        const d = new Date(ts); 
        return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`
    };

    const chartOption = useMemo(() => {
        if (!equity?.times) return {};
        
        // 此处逻辑：如果是 individual 模式，可以展示该股票的 K 线（假设数据源支持）
        // 这里暂时保留你原有的多曲线逻辑，但根据 selectedSymbol 突出显示
        const series = (equity.average || []).length > 0 ? [{
            name: "Portfolio", type: "line", smooth: true, showSymbol: false,
            data: equity.times.map((t: any, i: number) => [t, equity.average[i]])
        }] : [];

        // 添加详情曲线
        Object.entries(equity.details || {}).forEach(([symbol, arr]: any) => {
            series.push({
                name: symbol, type: "line", smooth: true, showSymbol: false,
                lineStyle: { 
                    width: symbol === selectedSymbol ? 3 : 1,
                    opacity: selectedSymbol && symbol !== selectedSymbol ? 0.2 : 1 
                },
                data: equity.times.map((t: any, i: number) => [t, arr[i]])
            });
        });

        return {
            tooltip: { trigger: "axis", backgroundColor: "rgba(0,0,0,0.7)", textStyle: { color: "#fff" } },
            grid: { top: 40, bottom: 60, left: 50, right: 20 },
            xAxis: { type: "time" },
            yAxis: { type: "value", scale: true },
            dataZoom: [{ type: "inside" }, { type: "slider" }],
            series
        };
    }, [equity, selectedSymbol, viewMode]);

    return (
        <FullScreenBox
            isFull={fullSection === 'chart'}
            onToggle={() => setFullSection(fullSection === 'chart' ? null : 'chart')}
            sx={{ height: "100%", width: "100%" }}
        >
            <ReactECharts
                option={chartOption}
                style={{ height: "100%", width: "100%" }}
                onEvents={{
                    click: (p: any) => setSelectedSymbol(p.seriesName === "Portfolio" ? null : replaceEmji(p.seriesName))
                }}
            />
        </FullScreenBox>
    );
};