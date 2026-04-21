import { escapeSQL, joinSQL, toSQLList, type Preset } from "../../utils/sqlUtils"

type QueryPlan = {
    from: string
    selects: string[]
    joins: JoinNode[]
    wheres: string[]
    orderBy: string[]
}

type JoinNode = {
    key: string          // 用于去重
    sql: string
    priority: number     // 优化排序
}

export function buildPlan(p: Preset): QueryPlan {

    const plan: QueryPlan = {
        from: "stock_daily s",
        selects: ["s.*"],
        joins: [],
        wheres: [],
        orderBy: ["s.date", "s.symbol"]
    }

    // =========================
    // 🥇 Universe（高优先级 JOIN）
    // =========================
    if (p?.universe) {
        plan.joins.push({
            key: `universe:${p.universe}`,
            priority: 1,
            sql: joinSQL([
                "JOIN universe_map u ON s.symbol = u.symbol",
                `AND u.universe = '${escapeSQL(p.universe)}'`,
                "AND (u.end_date IS NULL OR s.date <= u.end_date)",
                "AND s.date >= u.start_date"
            ])
        })
    }

    // =========================
    // 🥈 Sector
    // =========================
    if (p?.sectors?.length) {
        plan.joins.push({
            key: "sector",
            priority: 2,
            sql: joinSQL([
                "JOIN stock_sector ss ON s.symbol = ss.symbol",
                `AND ss.sector IN (${toSQLList(
                    p.sectors.map(s => s.replace("SEC_", "").toLowerCase())
                )})`,
                "AND (ss.end_date IS NULL OR s.date <= ss.end_date)",
                "AND s.date >= ss.start_date"
            ])
        })
    }

    // =========================
    // 🥉 Filters
    // =========================
    if (p?.markets?.length) {
        plan.wheres.push(`s.market IN (${toSQLList(p.markets)})`)
    }

    if (p?.symbols?.length) {
        plan.wheres.push(`s.symbol IN (${toSQLList(p.symbols)})`)
    }

    // =========================
    // ⏱ Time（必须）
    // =========================
    plan.wheres.push(`s.date >= '${p?.start}'`)
    plan.wheres.push(`s.date <= '${p?.end}'`)

    return plan
}

export function optimizePlan(plan: QueryPlan): QueryPlan {

    // 1️⃣ JOIN 去重
    const joinMap = new Map<string, JoinNode>()
    for (const j of plan.joins) {
        joinMap.set(j.key, j)
    }

    let joins = Array.from(joinMap.values())

    // 2️⃣ JOIN 排序（优先级低的后执行）
    joins = joins.sort((a, b) => a.priority - b.priority)

    // 3️⃣ WHERE 去重
    const wheres = Array.from(new Set(plan.wheres))

    return {
        ...plan,
        joins,
        wheres
    }
}

export function compileSQL(plan: QueryPlan) {

    return joinSQL([
        "SELECT " + plan.selects.join(", "),
        "FROM " + plan.from,
        ...plan.joins.map(j => j.sql),
        plan.wheres.length
            ? "WHERE " + plan.wheres.join("\nAND ")
            : "",
        "ORDER BY " + plan.orderBy.join(", ")
    ])
}