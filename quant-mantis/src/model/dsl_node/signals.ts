import { NodeType, NodeDType, PortfolioContext } from "./types";
import { Node, FeatureNode } from "./nodes";

export enum SignalGroup {
    Null = -1,
    TS = 1,
    CS = 2,
    TS_CS = 4,
}

// 信号组枚举已在 types.ts 定义
export abstract class Signal extends FeatureNode {
    signalGroup: SignalGroup;

    constructor(name: string | null = null, signalGroup: SignalGroup = SignalGroup.Null) {
        super(NodeType.Signal);
        this.dtype = NodeDType.SIGNAL;
        this._name = name || this.constructor.name;
        this.signalGroup = signalGroup;
    }

    static build(_nodeExpr: string | any | Node): Signal {
        // 忽略构建逻辑，返回空实现
        return new SignalWrapper(null, {} as Node);
    }

    get name(): string {
        return this._name;
    }

    _args(): any[] {
        return [this.signalGroup, ...super._args()];
    }

    isGroup(signalGroups: number): boolean {
        return (this.signalGroup & signalGroups) === this.signalGroup;
    }

    and(other: Signal): BinarySignalOp {
        return new BinarySignalOp(this, other, "and");
    }

    or(other: Signal): BinarySignalOp {
        return new BinarySignalOp(this, other, "or");
    }

    when(schedule: Signal): SignalGate {
        return new SignalGate(this, schedule);
    }

    filter(condition: Signal): SignalGate {
        return new SignalGate(this, condition);
    }

    confirm(condition: Signal): SignalGate {
        return new SignalGate(this, condition);
    }

    gate(other: Signal): SignalGate {
        return new SignalGate(this, other);
    }

    cross(b: FeatureNode): Cross {
        return new Cross(this, b);
    }

    crossunder(b: FeatureNode): CrossUnder {
        return new CrossUnder(this, b);
    }

    cooldown(n: number): Cooldown {
        return new Cooldown(this, n);
    }

    hold(n: number): Hold {
        return new Hold(this, n);
    }
}

// 信号包装器
export class SignalWrapper extends Signal {
    node: Node;

    constructor(name: string | null, node: Node, group: SignalGroup = SignalGroup.TS_CS) {
        super(name, group);
        if (node.dtype !== NodeDType.BOOL) {
            throw new Error("wrong node dtype, expect bool type for Signal");
        }
        this.dtype = node.dtype;
        this.node = node;
    }

    _args(): any[] {
        return [this.node.cacheKey(), ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 时间序列信号
export class TSSignal extends Signal {
    constructor() {
        super(null, SignalGroup.TS);
    }
}

// 冷却信号
export class Cooldown extends TSSignal {
    signal: Signal;
    n: number;

    constructor(signal: Signal, n: number) {
        super();
        this.signal = signal;
        this.n = n;
    }

    _args(): any[] {
        return [this.signal.cacheKey(), this.n, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 持有信号
export class Hold extends TSSignal {
    signal: Signal;
    n: number;

    constructor(signal: Signal, n: number) {
        super();
        this.signal = signal;
        this.n = n;
    }

    _args(): any[] {
        return [this.signal.cacheKey(), this.n, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 复合信号
export abstract class ComplexSignal extends Signal {
    left: Signal;
    right: Signal;

    constructor(left: Signal, right: Signal) {
        super();
        this.left = left;
        this.right = right;
    }

    _args(): any[] {
        return [this.left.cacheKey(), this.right.cacheKey(), ...super._args()];
    }
}

// 信号门控
export class SignalGate extends ComplexSignal {
    constructor(signal: Signal, signalGate: Signal) {
        super(signal, signalGate);
        this.signalGroup = SignalGroup.TS_CS;
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 二进制信号操作
export class BinarySignalOp extends ComplexSignal {
    op: string;

    constructor(left: Signal, right: Signal, op: string) {
        super(left, right);
        this.signalGroup = SignalGroup.TS_CS;
        this.op = op;
    }

    _args(): any[] {
        return [this.left.cacheKey(), this.right.cacheKey(), this.op, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 上穿信号
export class Cross extends ComplexSignal {
    constructor(left: FeatureNode, right: FeatureNode) {
        super(left as Signal, right as Signal);
        if (left.dtype !== NodeDType.NUMERIC || right.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("Cross requires numeric inputs");
        }
        this.signalGroup = SignalGroup.TS_CS;
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 下穿信号
export class CrossUnder extends ComplexSignal {
    constructor(left: FeatureNode, right: FeatureNode) {
        super(left as Signal, right as Signal);
        this.signalGroup = SignalGroup.TS_CS;
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 截面信号
export class CSSignal extends Signal {
    constructor() {
        super(null, SignalGroup.CS);
    }
}

// 每日再平衡
export class RebalanceDaily extends CSSignal {
    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 每周再平衡
export class RebalanceWeekly extends CSSignal {
    weekday: number;

    constructor(weekday: number = 0) {
        super();
        this.weekday = weekday;
    }

    _args(): any[] {
        return [this.weekday, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 每月再平衡
export class RebalanceMonthly extends CSSignal {
    day: number;

    constructor(day: number = 1) {
        super();
        this.day = day;
    }

    _args(): any[] {
        return [this.day, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 每N天再平衡
export class RebalanceEveryNDays extends CSSignal {
    n: number;

    constructor(n: number) {
        super();
        this.n = n;
    }

    _args(): any[] {
        return [this.n, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 指定日期再平衡
export class RebalanceOnDates extends CSSignal {
    dates: Date[];

    constructor(dates: Date[]) {
        super();
        this.dates = dates;
    }

    _args(): any[] {
        return [this.dates, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 月末再平衡
export class RebalanceMonthEnd extends CSSignal {
    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 周末再平衡
export class RebalanceWeekEnd extends CSSignal {
    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 注册函数（简化）
export function registerSignals(): void {
    // 忽略注册逻辑
}