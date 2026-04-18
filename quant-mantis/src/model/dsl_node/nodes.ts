import { NodeType, NodeDType, Scope, PortfolioContext } from "./types";

// 核心枚举和基础类型
// export enum NodeType {
//   Factor = "Factor",
//   Data = "Data",
//   Indicator = "Indicator",
//   Signal = "Signal",
//   Function = "Function"
// }

// export enum NodeDType {
//   NUMERIC = "NUMERIC",
//   BOOL = "BOOL",
//   SIGNAL = "SIGNAL",
//   ANY = "ANY",
//   FRAME = "FRAME",
//   NULL = "NULL"
// }

// export enum Scope {
//   TS = "TS",  // Time Series
//   CS = "CS"   // Cross Section
// }

// // ==========================
// // 上下文（仅占位，不实现）
// // ==========================
// export class PortfolioContext { }
// export class ExecutionEngine { }
// export class DataProvider { }

// ==========================
// Node 基类
// ==========================
export abstract class Node {
    _name: string;
    _type: NodeType;
    scope?: Scope;
    dtype?: NodeDType;

    constructor(name?: string, type: NodeType = NodeType.Unknown) {
        this._type = type;
        this._name = name || this.constructor.name;
    }

    _args(): any[] {
        return [this.name, this._type, this.dtype, this.scope];
    }

    cacheKey(): string {
        return JSON.stringify([this.constructor.name, ...this._args()]);
    }

    // ===== 类型判断 =====
    get type(): NodeType {
        return this._type;
    }

    get name(): string {
        return this._name;
    }

    get is_numeric(): boolean {
        return this.dtype === NodeDType.NUMERIC;
    }

    get is_bool(): boolean {
        return this.dtype === NodeDType.BOOL;
    }

    get is_signal(): boolean {
        return this.dtype === NodeDType.SIGNAL;
    }

    get is_frame(): boolean {
        return this.dtype === NodeDType.FRAME;
    }

    evaluate(data: any, context: PortfolioContext = new PortfolioContext(), return_result = false): any {
        // @ts-ignore
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        return_result;

        return this.compute(data, context);
    }

    compute(data: any, context: PortfolioContext): any {
        // @ts-ignore
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        data;
        context;
        throw new Error("Not implemented");
    }

    // =========================
    // DSL 操作符重载
    // =========================
    __add__(other: any): Node {
        return new BinaryOp(this, toNode(other), "add");
    }

    add(other: any): Node {
        return this.__add__(other);
    }

    __sub__(other: any): Node {
        return new BinaryOp(this, toNode(other), "sub");
    }

    sub(other: any): Node {
        return this.__sub__(other);
    }

    __mul__(other: any): Node {
        return new BinaryOp(this, toNode(other), "mul");
    }

    mul(other: any): Node {
        return this.__mul__(other);
    }

    __truediv__(other: any): Node {
        return new BinaryOp(this, toNode(other), "div");
    }

    div(other: any): Node {
        return this.__truediv__(other);
    }

    __gt__(other: any): Node {
        return new BinaryOp(this, toNode(other), "gt");
    }

    gt(other: any): Node {
        return this.__gt__(other);
    }

    __lt__(other: any): Node {
        return new BinaryOp(this, toNode(other), "lt");
    }

    lt(other: any): Node {
        return this.__lt__(other);
    }

    __ge__(other: any): Node {
        return new BinaryOp(this, toNode(other), "ge");
    }

    ge(other: any): Node {
        return this.__ge__(other);
    }

    __le__(other: any): Node {
        return new BinaryOp(this, toNode(other), "le");
    }

    le(other: any): Node {
        return this.__le__(other);
    }

    __eq__(other: any): Node {
        return new BinaryOp(this, toNode(other), "eq");
    }

    eq(other: any): Node {
        return this.__eq__(other);
    }

    __ne__(other: any): Node {
        return new BinaryOp(this, toNode(other), "ne");
    }

    ne(other: any): Node {
        return this.__ne__(other);
    }

    __and__(other: any): Node {
        return new BinaryOp(this, toNode(other), "and");
    }

    and(other: any): Node {
        return this.__and__(other);
    }

    __or__(other: any): Node {
        return new BinaryOp(this, toNode(other), "or");
    }

    or(other: any): Node {
        return this.__or__(other);
    }

    __invert__(): Node {
        return new UnaryOp(this, "not");
    }

    invert(): Node {
        return this.__invert__();
    }
    
    // 扩展方法
    slope(): Node {
        return new Slope(this);
    }
}

// =========================
// FeatureNode
// =========================
export class FeatureNode extends Node {
    constructor(type: NodeType = NodeType.Unknown) {
        super(undefined, type);
    }
}

// =========================
// 常量节点
// =========================
export class ConstNode extends FeatureNode {
    override dtype = NodeDType.NUMERIC;
    value: any;

    constructor(value: any) {
        super(NodeType.Indicator);
        this.value = value;
    }

    _args(): any[] {
        return [this.value, ...super._args()];
    }
}

// =========================
// to_node
// =========================
export function toNode(x: any): Node {
    if (x instanceof Node) return x;
    return new ConstNode(x);
}

// =========================
// ArgNode
// =========================
export class ArgNode extends ConstNode {
    override dtype = NodeDType.ANY;
    constructor(value: any) {
        super(value);
    }
}

// =========================
// to_args
// =========================
export function to_args(x: any): Node {
    if (typeof x === "object" && x?.value !== undefined) {
        x = x.value;
    }
    if (typeof x === "string") {
        return new ArgNode(x);
    }
    return toNode(x);
}

// =========================
// BinaryOp
// =========================
export class BinaryOp extends FeatureNode {
    left: Node;
    right: Node;
    op: string;

    constructor(left: Node, right: Node, op: string) {
        super();
        this.left = left;
        this.right = right;
        this.op = op;
        this.dtype = this._infer_dtype();
        this.scope = this._infer_scope();
    }

    _args(): any[] {
        return [this.left.cacheKey(), this.right.cacheKey(), this.op, ...super._args()];
    }

    _infer_dtype(): NodeDType {
        // @ts-ignore
        const l = this.left.dtype!;
        // @ts-ignore
        const r = this.right.dtype!;

        if (["add", "sub", "mul", "div"].includes(this.op)) {
            return NodeDType.NUMERIC;
        }

        if (["gt", "lt", "ge", "le", "eq", "ne"].includes(this.op)) {
            return NodeDType.BOOL;
        }

        if (["and", "or"].includes(this.op)) {
            return NodeDType.BOOL;
        }

        throw new Error("unknown op: " + this.op);
    }

    _infer_scope(): Scope {
        if (this.left.scope === Scope.CS || this.right.scope === Scope.CS) {
            return Scope.CS;
        }
        return Scope.TS;
    }
}

// =========================
// UnaryOp
// =========================
export class UnaryOp extends FeatureNode {
    override dtype = NodeDType.BOOL;
    node: Node;
    op: string;

    constructor(node: Node, op: string) {
        super(NodeType.Factor);
        this.node = node;
        this.op = op;
    }

    _args(): any[] {
        return [this.node.cacheKey(), this.op, ...super._args()];
    }
}

// =========================
// Slope（占位）
// =========================
export class Slope extends FeatureNode {
    constructor(node: Node) {
        // @ts-ignore
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        node;
        super();
    }
}