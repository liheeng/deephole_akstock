// expr_validater.ts
import { ExprParser } from "./node_expr_parser"
import { Node } from "./nodes"

export async function validateExpr(expr: string) {
  try {
    const node: Node =  new ExprParser().parse( expr )
    return node
  } catch (error) {
    throw error
  }
}

// export async function validateExpr(expr: string) {
//   const res = await fetch("/api/validate_expr", {
//     method: "POST",
//     body: JSON.stringify({ expr })
//   })

//   return res.json()
// }