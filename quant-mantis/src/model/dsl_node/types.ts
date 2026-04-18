// 核心枚举和基础类型
export enum NodeType {
    Unknown = "Unknown",
    Factor = "Factor",
    Data = "Data",
    Indicator = "Indicator",
    Signal = "Signal",
    Function = "Function"
}

export enum NodeDType {
    NUMERIC = "NUMERIC",
    BOOL = "BOOL",
    SIGNAL = "SIGNAL",
    ANY = "ANY",
    FRAME = "FRAME",
    NULL = "NULL"
}

export enum Scope {
    TS = "TS",  // Time Series
    CS = "CS"   // Cross Section
}

export enum SignalGroup {
    Null = -1,
    TS = 1,
    CS = 2,
    TS_CS = 4
}

// 基础上下文类型（简化）
export class PortfolioContext {
    private data: Record<string, any> = {};

    get(key: string): any {
        return this.data[key];
    }

    set(key: string, value: any): void {
        this.data[key] = value;
    }
}

// =========================
// NodeParam （严格对应 Python dataclass）
// =========================
export class NodeParam {
    name: string;
    type: string;
    default: any = null;
    desc: string = "";

    constructor(
        name: string,
        type: string,
        defaultVal: any = null,
        desc: string = ""
    ) {
        this.name = name;
        this.type = type;
        this.default = defaultVal;
        this.desc = desc;
    }
}

// =========================
// NodeMeta （严格对应 Python dataclass）
// =========================
export class NodeMeta {
    name: string;
    group: string;
    desc: string = "";
    params: NodeParam[] = [];

    constructor(
        name: string,
        group: string,
        desc: string = "",
        params: NodeParam[] = []
    ) {
        this.name = name;
        this.group = group;
        this.desc = desc;
        this.params = params;
    }
}