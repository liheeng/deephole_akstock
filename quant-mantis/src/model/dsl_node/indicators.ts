import { NodeType, NodeDType, Scope, PortfolioContext } from "./types";
import { FeatureNode } from "./nodes";

// 指标结果类
export class IndicatorResult {
    data: any;
    type: NodeDType;

    constructor(data: any, type: NodeDType) {
        this.data = data;
        this.type = type;
    }

    toSeries(): any {
        if (Array.isArray(this.data) || typeof this.data === "object") {
            return this.data;
        }
        throw new TypeError("Not a Series");
    }

    toFrame(): any {
        if (typeof this.data === "object" && !Array.isArray(this.data)) {
            return this.data;
        }
        throw new TypeError("Not a DataFrame");
    }

    toSignal(): boolean[] {
        if (this.type === NodeDType.SIGNAL) {
            return this.data.map((d: any) => Boolean(d));
        }
        throw new TypeError("Not a signal");
    }

    toString(): string {
        return `IndicatorResult(type=${this.type}, shape=${this.data?.length || 0})`;
    }
}

// 指标基类
export abstract class Indicator extends FeatureNode {
    outputType: NodeDType;

    constructor() {
        super(NodeType.Indicator);
        this.dtype = NodeDType.NUMERIC;
        this.scope = Scope.TS;
        this.outputType = NodeDType.NUMERIC;
    }

    compute(_data: any, _context: PortfolioContext): any {
        throw new Error("Not implemented");
    }

    evaluate(_data: any, _context: PortfolioContext = new PortfolioContext(), returnResult = false): any {
        const raw = super.evaluate(_data, _context);
        const indicatorResult = this._wrap(raw);

        if (returnResult) {
            return indicatorResult;
        }

        // switch (indicatorResult.type) {
        //     case NodeDType.NUMERIC:
        //         return indicatorResult.toSeries();
        //     case NodeDType.FRAME:
        //         return indicatorResult.toFrame();
        //     case NodeDType.SIGNAL:
        //         return indicatorResult.toSignal();
        //     default:
        //         return raw;
        // }
        const out = indicatorResult.data

        // 🔥 只做语义校验，不做结构转换
        if (indicatorResult.type == NodeDType.SIGNAL) {
            return out.map((d: any) => Boolean(d)).astype(Boolean)
        }
            

        return out
    }

    private _wrap(raw: any): IndicatorResult {
        if (this.outputType === NodeDType.SIGNAL) {
            raw = raw.map((d: any) => Boolean(d));
        }
        return new IndicatorResult(raw, this.outputType);
    }
}

// 移动平均线
export class MAIndicator extends Indicator {
    period: number;

    constructor(period: number) {
        super();
        this.period = period;
        this.outputType = NodeDType.NUMERIC;
    }

    get name(): string {
        return `ma${this.period}`;
    }

    _args(): any[] {
        return [this.period, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// RSI
export class RSIIndicator extends Indicator {
    period: number;

    constructor(period: number = 14) {
        super();
        this.period = period;
        this.outputType = NodeDType.NUMERIC;
    }

    get name(): string {
        return `rsi${this.period}`;
    }

    _args(): any[] {
        return [this.period];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// MACD
export class MacdIndicator extends Indicator {
    fastPeriod: number;
    slowPeriod: number;
    signalPeriod: number;

    constructor(fastPeriod = 12, slowPeriod = 26, signalPeriod = 9) {
        super();
        this.fastPeriod = fastPeriod;
        this.slowPeriod = slowPeriod;
        this.signalPeriod = signalPeriod;
        this.outputType = NodeDType.NUMERIC;
    }

    get name(): string {
        return `macd${this.fastPeriod}_${this.slowPeriod}_${this.signalPeriod}`;
    }

    _args(): any[] {
        return [this.fastPeriod, this.slowPeriod, this.signalPeriod, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// ATR
export class ATRIndicator extends Indicator {
    period: number;

    constructor(period: number = 14) {
        super();
        this.period = period;
        this.outputType = NodeDType.NUMERIC;
    }

    get name(): string {
        return `atr${this.period}`;
    }

    _args(): any[] {
        return [this.period, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 布林带（中轨）
export class BollIndicator extends Indicator {
    period: number;

    constructor(period: number = 20) {
        super();
        this.period = period;
        this.outputType = NodeDType.FRAME;
    }

    get name(): string {
        return `boll_mid_${this.period}`;
    }

    _args(): any[] {
        return [this.period, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 布林带（全轨）
export class BollFullIndicator extends Indicator {
    period: number;
    nStd: number;

    constructor(period: number = 20, nStd: number = 2) {
        super();
        this.period = period;
        this.nStd = nStd;
        this.outputType = NodeDType.FRAME;
    }

    get name(): string {
        return `boll_${this.period}_${this.nStd}`;
    }

    _args(): any[] {
        return [this.period, this.nStd, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 突破指标
export class BreakoutIndicator extends Indicator {
    period: number;

    constructor(period: number = 20) {
        super();
        this.period = period;
        this.outputType = NodeDType.SIGNAL;
    }

    get name(): string {
        return `breakout_${this.period}`;
    }

    _args(): any[] {
        return [this.period, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 完整突破指标
export class BreakoutFullIndicator extends Indicator {
    period: number;

    constructor(period: number = 20) {
        super();
        this.period = period;
        this.outputType = NodeDType.SIGNAL;
    }

    get name(): string {
        return `breakout_full_${this.period}`;
    }

    _args(): any[] {
        return [this.period, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 成交量均线
export class VolumeMAIndicator extends Indicator {
    period: number;

    constructor(period: number = 20) {
        super();
        this.period = period;
        this.outputType = NodeDType.NUMERIC;
    }

    get name(): string {
        return `vol_ma${this.period}`;
    }

    _args(): any[] {
        return [this.period, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 成交量突破
export class VolumeBreakoutIndicator extends Indicator {
    period: number;

    constructor(period: number = 20) {
        super();
        this.period = period;
        this.outputType = NodeDType.SIGNAL;
    }

    get name(): string {
        return `vol_breakout_${this.period}`;
    }

    _args(): any[] {
        return [this.period, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 注册函数（简化）
export function registerIndicators(): void {
    // 忽略注册逻辑
}