import { parse } from "acorn";
import type {Node as ASTNode} from "acorn";
// import { NodeDType } from "./types";
import { Node, ConstNode, toNode } from "./nodes";
import { NodeRegistry } from "./node_registry";

export class ExprParser {
  parse(exprStr: string): Node {
    const ast = parse(exprStr, { ecmaVersion: "latest" });
    return this._parseNode((ast as any).body[0].expression);
  }

  // =========================
  // Utils
  // =========================
  private _unwrap(x: any): any {
    if (x instanceof ConstNode) return x.value;
    if (Array.isArray(x)) return x.map((v) => this._unwrap(v));
    if (x && typeof x === "object" && x instanceof Node) {
      throw new Error(`Expected literal, got Node: ${x}`);
    }
    return x;
  }

  private _isNodeParam(param: { type: string }): boolean {
    const nodeTypes = ["Node", "Signal", "Factor", "Indicator", "Data"];
    return nodeTypes.includes(param.type);
  }

  // =========================
  // 主递归解析
  // =========================
  private _parseNode(node: ASTNode): Node {
    // 常量
    if (node.type === "Literal") {
      return toNode((node as any).value);
    }

    // 变量
    if (node.type === "Identifier") {
      return NodeRegistry.create((node as any).name);
    }

    // 二元运算
    if (node.type === "BinaryExpression") {
      const left = this._parseNode((node as any).left);
      const right = this._parseNode((node as any).right);
      const op = (node as any).operator;

      if (op === "+") return left.add(right);
      if (op === "-") return left.sub(right);
      if (op === "*") return left.mul(right);
      if (op === "/") return left.div(right);
      if (op === "&") return left.and(right);
      if (op === "|") return left.or(right);
    }

    // 比较
    if (node.type === "BinaryExpression" && ["<", ">", "<=", ">=", "==", "!="].includes((node as any).operator)) {
      const left = this._parseNode((node as any).left);
      const right = this._parseNode((node as any).right);
      const op = (node as any).operator;

      if (op === ">") return left.gt(right);
      if (op === "<") return left.lt(right);
      if (op === ">=") return left.ge(right);
      if (op === "<=") return left.le(right);
      if (op === "==") return left.eq(right);
      if (op === "!=") return left.ne(right);
    }

    // 逻辑运算 && ||
    if (node.type === "LogicalExpression") {
      const left = this._parseNode((node as any).left);
      const right = this._parseNode((node as any).right);
      const op = (node as any).operator;

      if (op === "&&") return left.and(right);
      if (op === "||") return left.or(right);
    }

    // 取反 ~
    if (node.type === "UnaryExpression" && (node as any).operator === "~") {
      const operand = this._parseNode((node as any).argument);
      return operand.invert();
    }

    // 函数调用
    if (node.type === "CallExpression") {
      const funcName = (node as any).callee.name;
      const meta = NodeRegistry.getMeta(funcName);
      if (!meta) throw new Error(`${funcName} not registered`);

      const args = (node as any).arguments.map((a: any) => this._parseNode(a));
      const kwargs: Record<string, any> = {};

      for (let i = 0; i < meta.params.length; i++) {
        const param = meta.params[i];
        let val = args[i] ?? param.default;
        if (val === undefined) throw new Error(`${funcName} missing param ${param.name}`);

        if (this._isNodeParam(param)) {
          if (!(val instanceof Node)) {
            throw new Error(`${funcName} ${param.name} expects Node`);
          }
          kwargs[param.name] = val;
        } else {
          const raw = this._unwrap(val);
          kwargs[param.name] = raw;
        }
      }

      return NodeRegistry.create(funcName, kwargs);
    }

    throw new Error(`Unsupported node: ${node.type}`);
  }
}