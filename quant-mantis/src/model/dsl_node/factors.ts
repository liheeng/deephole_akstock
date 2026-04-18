import { NodeType, NodeDType, Scope, PortfolioContext } from "./types";
import { Node, FeatureNode } from "./nodes";

// 因子基类
export class Factor extends FeatureNode {
    node: Node;


    constructor(name: string | null, exprStr: string | Node) {
        super(NodeType.Factor);
        this.dtype = NodeDType.NUMERIC;
        this.scope = Scope.TS
        this._name = name || this.constructor.name;

        // 忽略 NodeBuilder 构建逻辑，直接赋值
        this.node = typeof exprStr === "string" ? {} as Node : exprStr;

        if (this.type !== NodeType.Factor) {
            throw new Error("Node type must be Factor");
        }
    }

    get name(): string {
        return this._name;
    }

    _args(): any[] {
        return [this.name, this.node.cacheKey(), ...super._args()];
    }

    // 忽略 compute 业务逻辑
    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }

    // 忽略 score 业务逻辑
    score(_data: any, _context: PortfolioContext): any {
        return null;
    }

    rank(): Rank {
        return new Rank(this);
    }
}

// 布尔因子
export class BoolFactor extends Factor {
    constructor(name: string | null, exprStr: string | Node) {
        super(name, exprStr);
        this.dtype = NodeDType.BOOL;
    }
}

// 信号转因子
export class SignalToFactor extends Factor {
    constructor(name: string | null, signalNode: Node) {
        if (signalNode.dtype !== NodeDType.SIGNAL) {
            throw new TypeError("Expect signal");
        }
        super(name, signalNode);
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 因子包装器
export class FactorWrapper extends Factor {
    constructor(name: string | null, node: Node) {
        if (node.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("Expect numeric type of node");
        }
        super(name, node);
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 工具函数
export function wrapNumericNodeAsFactor(name: string | null, node: Node): Factor {
    if (node.type === NodeType.Signal && node.dtype === NodeDType.NUMERIC) {
        return new SignalToFactor(name, node);
    }
    return new FactorWrapper(name, node);
}

// 通用因子
export class GeneralFactor extends Factor {
    constructor(name: string, exprStr: string) {
        super(name, exprStr);
    }
}

// 排名类（移至此处，对应 Python 中 functions.py 的 Rank）
export class Rank extends FeatureNode {
    node: Node;
    scope = Scope.CS

    constructor(node: Node) {
        super(NodeType.Factor);
        this.dtype = NodeDType.NUMERIC;

        if (node.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("rank() requires numeric input");
        }
        this.node = node;
    }

    _args(): any[] {
        return [this.node.cacheKey(), ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 注册函数（简化）
export function registerFactors(): void {
    // 忽略注册逻辑，仅保留函数结构
}