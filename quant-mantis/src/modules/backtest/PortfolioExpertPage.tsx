// src/App.tsx
import { useState } from 'react';
import { Container, Paper, Box, Button, Alert, Snackbar } from '@mui/material';
import Editor from '@monaco-editor/react';
// import BacktestResults from '../components/BacktestResults';
// import { runBacktest } from './lib/backtestApi';
import { PortfolioBuilder } from '../../components/portfolio/PortfolioBuilder';

const defaultCode = `// 回测策略构建示例
const builder = new PortfolioBuilder("我的策略", "signal_strategy");

builder
  .addStrategy("趋势跟踪")
    .addFactor("close > ma(20)")
    .addFactor("volume > vol_ma(20)")
    .setStrategySignal("cross(close, ma(10))")
    .setStrategyMode("long_only")
    .setStrategyThreshold(0.02)
  .endStrategy()
  .addStrategy("均值回归")
    .addFactor("rsi(14) < 30")
    .setStrategyTopN(5)
    .setStrategyMode("long_short")
  .endStrategy()
  .setStrategyOp("or")
  .setScheduleSignal("rebalance_daily()")
  .setPortfolioParams({ initial_capital: 100000, commission: 0.001 });

const config = builder.build();
console.log("策略配置:", config);
config; // 返回配置对象用于提交
`;

export interface BacktestResult {
  stats: {
    total_return: number;
    annual_return: number;
    sharpe_ratio: number;
    max_drawdown: number;
    win_rate: number;
    total_trades: number;
    [key: string]: number;
  };
  equity: Array<{ date: string; value: number }>;
  trades: Array<{
    entry_date: string;
    exit_date: string;
    symbol: string;
    direction: string;
    quantity: number;
    entry_price: number;
    exit_price: number;
    pnl: number;
    return_pct: number;
  }>;
}

export async function runBacktest(config: any): Promise<BacktestResult> {
  const response = await fetch('/api/backtest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config),
  });
  
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`回测失败: ${error}`);
  }
  
  return response.json();
}

export default function PortfolioExpertPage() {
  const [code, setCode] = useState(defaultCode);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  console.log(results)

  // 执行用户代码，提取配置
  const executeCode = async () => {
    setLoading(true);
    setError(null);
    try {
      // 创建沙箱执行环境
      const sandbox = {
        PortfolioBuilder,
        console: { log: (...args: any[]) => console.log('[User]', ...args) },
      };
      
      // 执行用户代码并获取最后一个表达式的值（配置对象）
      const func = new Function(...Object.keys(sandbox), `"use strict";\n${code}\n return config;`);
      const config = func(...Object.values(sandbox));
      
      if (!config || typeof config !== 'object') {
        throw new Error('代码必须返回一个有效的配置对象');
      }
      
      // 提交后端
      const result = await runBacktest(config);
      setResults(result);
    } catch (err: any) {
      setError(err.message || '代码执行失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Container maxWidth="xl" sx={{ py: 4 }}>
      <Paper elevation={3} sx={{ p: 2, mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <h2>📊 量化回测专家编辑模式</h2>
          <Button 
            variant="contained" 
            onClick={executeCode} 
            disabled={loading}
            startIcon={loading ? <span>⏳</span> : <span>▶️</span>}
          >
            {loading ? '回测中...' : '运行回测'}
          </Button>
        </Box>
        
        <Editor
          height="500px"
          defaultLanguage="javascript"
          value={code}
          onChange={(value) => setCode(value || '')}
          theme="vs-dark"
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            lineNumbers: 'on',
            automaticLayout: true,
            suggestOnTriggerCharacters: true,
            quickSuggestions: true,
          }}
          onMount={(editor, monaco) => {
            // 添加自定义类型声明
            monaco.languages.typescript.javascriptDefaults.addExtraLib(
              `declare class PortfolioBuilder {
                constructor(name: string, portfolioMode: 'signal_strategy' | 'weight_strategy');
                addStrategy(name: string): this;
                addFactor(factorExpr: string): this;
                setStrategySignal(signal: string): this;
                setStrategyMode(mode: 'long_only' | 'short_only' | 'long_short'): this;
                setStrategyThreshold(threshold: number): this;
                setStrategyTopN(topN: number): this;
                endStrategy(): this;
                setScheduleSignal(signal: string): this;
                setStrategyOp(op: 'and' | 'or'): this;
                setStrategyWeights(weights: number[]): this;
                setVoteWeights(weights: number[]): this;
                setPortfolioParams(params: Record<string, any>): this;
                build(): any;
              }`,
              'global:portfolio-builder'
            );
          }}
        />
      </Paper>

      {/* {results && <BacktestResults results={results} />} */}
      
      <Snackbar open={!!error} autoHideDuration={6000} onClose={() => setError(null)}>
        <Alert severity="error">{error}</Alert>
      </Snackbar>
    </Container>
  );
}