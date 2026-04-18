import { NodeType, NodeDType, PortfolioContext } from "./types";
import { Node } from "./nodes";

// 数据节点基类
export class DataNode extends Node {

    constructor(type: NodeType = NodeType.Data) {
        super(type);
        this.dtype = NodeDType.ANY;
    }
}

// 原始数据节点
export class RawDataNode extends DataNode { }

// 价格节点
export class Price extends RawDataNode {
    column: string;

    constructor(column: string = "close") {
        super(NodeType.Data);
        this.dtype = NodeDType.NUMERIC;
        this.column = column;
    }

    get name(): string {
        return `price_${this.column}`;
    }

    _args(): any[] {
        return [this.column, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 数据库节点
export class DBNode extends RawDataNode {
    fieldName: string;

    constructor(fieldName: string) {
        super();
        this.dtype = NodeDType.ANY;
        this.fieldName = fieldName;
    }

    get name(): string {
        return this.fieldName;
    }

    _args(): any[] {
        return [this.fieldName, ...super._args()];
    }

    compute(_data: any, _context: PortfolioContext): any {
        return null;
    }
}

// 注册函数（简化）
export function registerData(): void {
    // 忽略注册逻辑
}