# Summary
## 量化交易常用表情符

这里给你一套完整、专业、量化交易专用的 Emoji 清单，按模块分好，直接复制就能用在你的 React + MUI 项目里，风格统一、不杂乱、适合回测平台、策略系统、风控面板。

### 一、策略 & 核心模块

``` bash
🧠 Strategy 策略
🤖 Algo 算法
🔁 Model 模型
🎯 Signal 信号
📌 Rule 规则
🧪 Factor 因子
📍 Condition 条件
🧬 Logic 逻辑
```

### 二、回测 & 分析

``` bash
📊 Backtest 回测
📈 Return 收益
📉 Drawdown 回撤
📐 Index 指标
🧮 Calculation 计算
📋 Report 报告
🔍 Analysis 分析
📅 History 历史
```

### 三、投资组合 & 持仓

``` bash
📂 Portfolio 组合
🚀 Position 持仓
🧺 Assets 资产
📦 Position Size 仓位
🔄 Rebalance 再平衡
📉 Hedge 对冲
📈 Long 多头
📉 Short 空头
```

### 四、交易 & 执行

``` bash
⚙️ Operator 算子 / 执行器
📝 Order 订单
✅ Fill 成交
🕒 Time-Weighted 时间加权
🔁 TWAP / VWAP 算法单
📤 Submit 委托
📥 Execution 执行
🧾 Trade 交易
```

### 五、资金 & 收益

``` bash
💰 Capital 本金
💵 Cash 现金
📈 PnL 盈亏
📉 Loss 亏损
📈 Profit 盈利
📊 Yield 收益率
🎯 Benchmark 基准
📉 Fees 手续费
```

### 六、风险 & 风控

``` bash
🛡️ Risk 风险
📉 Volatility 波动率
📐 Sharpe 夏普比率
📐 Calmar 卡玛比率
📐 Sortino 索提诺比率
🛑 Stop Loss 止损
🛡️ Margin 保证金
📉 Max Drawdown 最大回撤
```

### 七、数据 & 因子

``` bash
📥 Dataset 数据集
🔢 Data 数据
📅 Bar K 线 / Bar 数据
📊 Tick 逐笔数据
🧪 Factor 因子
📊 Market 行情
📉 Price 价格
📈 Volume 成交量
```

### 八、系统 & 运行

``` bash
⚙️ System 系统
🤖 Auto 自动化
🕒 Schedule 调度
🧩 Pipeline 流程
📡 Real-time 实时
📊 Dashboard 看板
🧪 Simulation 模拟
🚀 Run 运行
```

## 直接可复制的 JSX 成品（你直接贴进代码）
```jsx
// 页面 / 卡片大标题
<Typography variant="h6">📊 回测分析 Backtest Analysis</Typography>
<Typography variant="h6">🧠 策略管理 Strategy</Typography>
<Typography variant="h6">📂 投资组合 Portfolio</Typography>
<Typography variant="h6">🎯 交易信号 Signal</Typography>
<Typography variant="h6">⚙️ 运行算子 Operator</Typography>
<Typography variant="h6">📥 数据集 Dataset</Typography>
<Typography variant="h6">📈 收益表现 Return</Typography>
<Typography variant="h6">📉 风险回撤 Drawdown</Typography>

// Chip 小标签（直接用）
<Chip label="📊 回测" size="small" />
<Chip label="🧠 策略" size="small" />
<Chip label="📂 组合" size="small" />
<Chip label="🎯 信号" size="small" />
<Chip label="⚙️ 算子" size="small" />
<Chip label="📥 数据" size="small" />
<Chip label="📈 收益" size="small" />
<Chip label="📉 回撤" size="small" />
<Chip label="💰 资金" size="small" />
<Chip label="🕒 周期" size="small" />
<Chip label="📐 指标" size="small" />
<Chip label="🔢 因子" size="small" />

// Chip 标签
<NestedChip size="small" label="🧠 Strategy" />
<NestedChip size="small" label="📊 Backtest" />
<NestedChip size="small" label="📂 Portfolio" />
<NestedChip size="small" label="🎯 Signal" />
<NestedChip size="small" label="⚙️ Operator" />
<NestedChip size="small" label="📈 Return" />
<NestedChip size="small" label="📉 Drawdown" />
<NestedChip size="small" label="💰 Capital" />
<NestedChip size="small" label="📐 Sharpe" />
<NestedChip size="small" label="🔢 Factor" />
<NestedChip size="small" label="🚀 Position" />
```
