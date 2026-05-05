// src/lib/PortfolioBuilder.ts
// import { Node } from '../../model/dsl_node/nodes'; // 从现有移植代码导入
import { ExprParser } from '../../model/dsl_node/node_expr_parser';

export enum StrategyMode {
  LONG_ONLY = 'long_only',
  SHORT_ONLY = 'short_only',
  LONG_SHORT = 'long_short'
}

export enum StrategyOp {
  AND = 'and',
  OR = 'or'
}

export interface StrategyConfig {
  name: string;
  factors: string[];
  signal?: string;
  strategyMode: StrategyMode;
  threshold?: number;
  topN?: number;
}

export interface PortfolioConfig {
  name: string;
  portfolioMode: 'signal_strategy' | 'weight_strategy';
  strategies: StrategyConfig[];
  strategyOp: StrategyOp;
  scheduleSignal?: string;
  strategyWeights?: number[];
  voteWeights?: number[];
  portfolioParams?: Record<string, any>;
}

export class PortfolioBuilder {
  private config: PortfolioConfig;
  private currentStrategy: StrategyConfig | null = null;

  constructor(name: string, portfolioMode: 'signal_strategy' | 'weight_strategy') {
    this.config = {
      name,
      portfolioMode,
      strategies: [],
      strategyOp: StrategyOp.AND,
    };
  }

  addStrategy(name: string): this {
    this.currentStrategy = {
      name,
      factors: [],
      strategyMode: StrategyMode.LONG_ONLY,
    };
    this.config.strategies.push(this.currentStrategy);
    return this;
  }

  addFactor(factorExpr: string): this {
    if (!this.currentStrategy) throw new Error('No active strategy');
    // 可选：使用ExprParser验证表达式合法性
    try {
      new ExprParser().parse(factorExpr);
    } catch (e) {
      console.warn('Expression validation warning:', e);
    }
    this.currentStrategy.factors.push(factorExpr);
    return this;
  }

  setStrategySignal(signal: string): this {
    if (this.currentStrategy) this.currentStrategy.signal = signal;
    return this;
  }

  setStrategyMode(mode: StrategyMode | string): this {
    if (this.currentStrategy) {
      this.currentStrategy.strategyMode = typeof mode === 'string' 
        ? StrategyMode[mode.toUpperCase() as keyof typeof StrategyMode] 
        : mode;
    }
    return this;
  }

  setStrategyThreshold(threshold: number): this {
    if (this.currentStrategy) this.currentStrategy.threshold = threshold;
    return this;
  }

  setStrategyTopN(topN: number): this {
    if (this.currentStrategy) this.currentStrategy.topN = topN;
    return this;
  }

  endStrategy(): this {
    this.currentStrategy = null;
    return this;
  }

  setScheduleSignal(signal: string): this {
    this.config.scheduleSignal = signal;
    return this;
  }

  setStrategyOp(op: 'and' | 'or'): this {
    this.config.strategyOp = op === 'and' ? StrategyOp.AND : StrategyOp.OR;
    return this;
  }

  setStrategyWeights(weights: number[]): this {
    this.config.strategyWeights = weights;
    return this;
  }

  setVoteWeights(weights: number[]): this {
    this.config.voteWeights = weights;
    return this;
  }

  setPortfolioParams(params: Record<string, any>): this {
    this.config.portfolioParams = params;
    return this;
  }

  build(): PortfolioConfig {
    return this.config;
  }
}