import { type PlanNode } from "./parseDuckDBExplain"
import { asNumber } from "./Strings"
// export type ExplainNode = {
//     type: string
//     rows?: number
//     extra?: string[]
//     children?: ExplainNode[]
// }

export type ExplainNode = PlanNode & {
    type: string;
};

export type ExplainHint = {
    level: "info" | "warning" | "error"
    message: string
}

export function analyzeExplain(tree: ExplainNode[]): ExplainHint[] {

    const hints: ExplainHint[] = []

    let totalJoins = 0
    let hasFilter = false

    function walk(node: ExplainNode) {

        // =========================
        // SEQ SCAN
        // =========================
        if (node.type === "SEQ_SCAN") {

            if ((asNumber(node.rows) || 0) > 1_000_000) {
                hints.push({
                    level: "warning",
                    message: `Sequential Scan on large table (~${node.rows?.toLocaleString()} rows)`
                })

                hints.push({
                    level: "info",
                    message: "💡 Add filters (date / market / symbol) or use indexed columns"
                })
            }

            if (node.extra?.some(x => x.includes("Filters"))) {
                hasFilter = true
            }
        }

        // =========================
        // ORDER BY
        // =========================
        if (node.type === "ORDER_BY" && (asNumber(node.rows) || 0) > 1_000_000) {
            hints.push({
                level: "warning",
                message: "Large ORDER BY detected"
            })
            hints.push({
                level: "info",
                message: "💡 Consider removing ORDER BY or reducing dataset size"
            })
        }

        // =========================
        // JOIN
        // =========================
        if (typeof node.type === "string" && node.type.includes("JOIN")) {
            totalJoins++
        }

        // =========================
        // Rows explosion
        // =========================
        if ((asNumber(node.rows) || 0) > 5_000_000) {
            hints.push({
                level: "warning",
                message: `Large intermediate result (~${node.rows?.toLocaleString()} rows)`
            })
        }

        node.children?.forEach(walk as any)
    }

    tree.forEach(walk)

    // =========================
    // Global rules
    // =========================
    if (!hasFilter) {
        hints.push({
            level: "error",
            message: "No WHERE filters detected"
        })
    }

    if (totalJoins >= 3) {
        hints.push({
            level: "warning",
            message: `Too many JOINs (${totalJoins})`
        })
        hints.push({
            level: "info",
            message: "💡 Consider precomputing universe/sector mapping"
        })
    }

    if (hints.length === 0) {
        hints.push({
            level: "info",
            message: "Query looks good 👍"
        })
    }

    return hints
}