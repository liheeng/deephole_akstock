import { NodeParam, NodeMeta } from "./types";
import { Node } from "./nodes";

export class NodeRegistry {
    private static _factories: Record<string, (...args: any[]) => Node> = {};
    private static _meta: Record<string, NodeMeta> = {};
    private static _groups: Record<string, string[]> = {};

    // 注册
    static register(
        name: string,
        factory: (...args: any[]) => Node,
        meta: NodeMeta
    ) {
        this._factories[name] = factory;
        this._meta[name] = meta;
        this._groups[meta.group] ||= [];
        this._groups[meta.group].push(name);
    }

    // 创建节点（支持 args + kwargs）
    static create(name: string, ...args: any[]): Node {
        const factory = this._factories[name];
        const meta = this._meta[name];
        if (!factory || !meta) throw new Error(`Node ${name} not registered`);

        try {
            const factoryFunc = this._lambdaStringToFunction(factory.toString());
                
            // 支持对象传参：create("MACD", { fast_period: 12 })
            if (args.length === 1 && typeof args[0] === "object" && args[0] !== null) {
                const kw = args[0];
                const finalArgs = meta.params.map((p) => kw[p.name] ?? p.default);
                return factoryFunc(...finalArgs);
            }

            // 普通顺序参数
            return factoryFunc(...args);
        } catch (e) {
            throw new Error(`create ${name} error: ${e}`);
        }
    }

    // ==============================
    // toDict（你现在的版本）
    // ==============================
    static toDict() {
        const result: Record<string, any[]> = {};
        for (const group in this._groups) {
            result[group] = [];
            for (const name of this._groups[group]) {
                const meta = this._meta[name];
                result[group].push({
                    name: name,
                    factory: (this._factories[name] as any).toString(),
                    desc: meta.desc,
                    params: meta.params.map((p) => ({
                        name: p.name,
                        type: p.type,
                        default: p.default,
                        desc: p.desc,
                    })),
                });
            }
        }
        return result;
    }

    // ==============================
    // ✅ ✅ ✅ fromDict 终极版（使用 factory 字符串）
    // ==============================
    static fromDict(dict: Record<string, any[]>) {
        this._factories = {};
        this._meta = {};
        this._groups = {};

        for (const group in dict) {
            for (const item of dict[group]) {
                const { name, factory, desc, params } = item;

                // 1. 重建参数 & meta
                const nodeParams = params.map(
                    (p: any) => new NodeParam(p.name, p.type, p.default, p.desc)
                );
                const meta = new NodeMeta(p.name, group, desc, nodeParams);

                // 2. ✅ 把 Python lambda → JS 函数
                const factoryFunc = this._lambdaStringToFunction(factory);

                // 3. 注册
                this.register(name, factoryFunc, meta);
            }
        }
    }

    /**
     * 把 Python lambda 字符串 转换成 JS/TS 可执行函数
     * 例如：
     * Python: lambda fast_period=12: MacdIndicator(fast_period)
     * JS:     (fast_period=12) => MacdIndicator(fast_period)
     */
    /**
     * 【完美修复】Python lambda → JS 箭头函数
     * 解决：Uncaught SyntaxError: Malformed arrow function parameter list
     */
    private static _lambdaStringToFunction(lambdaStr: string): (...args: any[]) => Node {
        if (!lambdaStr.includes('lambda')) {
            throw new Error('Not a lambda string');
        }

        // 去掉 lambda
        const withoutLambda = lambdaStr.replace(/^\s*lambda\s*/, '').trim();

        // 支持空参数
        const match = withoutLambda.match(/^([\s\S]*?)\s*:\s*(.+)$/);
        if (!match)
            throw new Error(`Invalid lambda string: ${lambdaStr}`);

        let [, paramsPart, bodyPart] = match;

        // 如果参数为空，则变成空括号
        paramsPart = paramsPart.trim();
        if (paramsPart === '') paramsPart = '()';

        // 否则加括号包裹（安全）
        else paramsPart = `(${paramsPart})`;

        const fnStr = `${paramsPart} => ${bodyPart.trim()}`;

        try {
            return new Function(`return ${fnStr}`)();
        } catch (err) {
            throw new Error(`Failed to parse lambda: ${lambdaStr}\n${err}`);
        }
    }

    // 工具方法
    static getMeta(name: string) { return this._meta[name]; }
    static listGroups() { return this._groups; }
    static getGroup(group: string) { return this._groups[group] || []; }
    static clear() {
        this._factories = {};
        this._meta = {};
        this._groups = {};
    }
}