import { NodeType, NodeDType, Scope, PortfolioContext } from "./types";
import { FeatureNode } from "./nodes";

// 函数基类
export abstract class Function extends FeatureNode {
    constructor() {
        super(NodeType.Factor);
    }
}

// 交叉函数
export class Cross extends Function {
    left: FeatureNode;
    right: FeatureNode;

    constructor(left: FeatureNode, right: FeatureNode) {
        super();
        this.dtype = NodeDType.BOOL;

        if (left.dtype !== NodeDType.NUMERIC || right.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("Cross requires numeric inputs");
        }
        this.left = left;
        this.right = right;
    }

    _args(): any[] {
        return [this.left.cacheKey(), this.right.cacheKey(), ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// TopN 函数
export class Top extends Function {
    window: any;
    node: FeatureNode;

    constructor(node: FeatureNode, window: any) {
        super();
        this.scope = Scope.CS;
        this.dtype = NodeDType.BOOL;
        this.window = window;
        this.node = node;
    }

    _args(): any[] {
        const windowKey = (this.window as any).cacheKey ? (this.window as any).cacheKey() : this.window;
        return [this.node.cacheKey(), windowKey, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 延迟函数
export class Delay extends Function {
    node: FeatureNode;
    window: any;

    constructor(node: FeatureNode, window: any) {
        super();
        if (node.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("Delay requires numeric input");
        }
        this.node = node;
        this.window = window;
    }

    _args(): any[] {
        const windowKey = (this.window as any).cacheKey ? (this.window as any).cacheKey() : this.window;
        return [this.node.cacheKey(), windowKey, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 均值函数
export class Mean extends Function {
    node: FeatureNode;
    window: any;

    constructor(node: FeatureNode, window: any) {
        super();
        this.dtype = NodeDType.NUMERIC;

        if (node.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("Mean requires numeric input");
        }
        this.node = node;
        this.window = window;
    }

    _args(): any[] {
        const windowKey = (this.window as any).cacheKey ? (this.window as any).cacheKey() : this.window;
        return [this.node.cacheKey(), windowKey, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// ZScore（截面）
export class ZScore extends Function {
    node: FeatureNode;

    constructor(node: FeatureNode) {
        super();
        this.scope = Scope.CS;
        this.dtype = NodeDType.NUMERIC;

        if (node.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("ZScore requires numeric input");
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

// ZScore（时间序列）
export class ZScoreTS extends Function {
    node: FeatureNode;
    window: number;

    constructor(node: FeatureNode, window: number) {
        super();
        this.scope = Scope.TS;
        this.dtype = NodeDType.NUMERIC;

        if (node.dtype !== NodeDType.NUMERIC) {
            throw new TypeError("ZScoreTS requires numeric input");
        }
        this.node = node;
        this.window = window;
    }

    _args(): any[] {
        return [this.node.cacheKey(), this.window, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 注册函数（简化）
export function registerFunctions(): void {
    // 忽略注册逻辑
}