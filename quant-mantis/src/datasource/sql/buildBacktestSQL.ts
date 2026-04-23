import { escapeSQL, joinSQL, toSQLList } from "../../utils/sqlUtils"
import { buildPlan, optimizePlan, compileSQL } from "./queryPlan"
import { type Preset } from "../../store/dataset.store"

export function buildBacktestSQL(p: Preset) {

    const where: string[] = []
    const joins: string[] = []

    const from = "stock_daily s"

    // =========================
    // 🥇 Universe（时间感知）
    // =========================
    if (p.universe) {
        joins.push(
            joinSQL([
                "JOIN universe_map u ON s.symbol = u.symbol",
                `AND u.universe = '${escapeSQL(p.universe)}'`,
                "AND (u.end_date IS NULL OR s.date <= u.end_date)",
                "AND s.date >= u.start_date"
            ])
        )
    }

    // =========================
    // 🥈 Sector（时间感知）
    // =========================
    if (p.sectors?.length) {

        const sectorValues = p.sectors.map(s =>
            `'${escapeSQL(s.replace("SEC_", "").toLowerCase())}'`
        ).join(", ")

        joins.push(
            joinSQL([
                "JOIN stock_sector ss ON s.symbol = ss.symbol",
                `AND ss.sector IN (${sectorValues})`,
                "AND (ss.end_date IS NULL OR s.date <= ss.end_date)",
                "AND s.date >= ss.start_date"
            ])
        )
    }

    // =========================
    // 🥉 Filters
    // =========================
    if (p.markets?.length) {
        where.push(`s.market IN (${toSQLList(p.markets)})`)
    }

    if (p.symbols?.length) {
        where.push(`s.symbol IN (${toSQLList(p.symbols)})`)
    }

    // =========================
    // ⏱ Date（强制）
    // =========================
    where.push(`s.date >= '${p.start}'`)
    where.push(`s.date <= '${p.end}'`)

    // =========================
    // 🧱 Final SQL（无污染）
    // =========================
    return joinSQL([
        "SELECT s.*",
        `FROM ${from}`,
        ...joins,
        "WHERE " + where.join("\nAND "),
        "ORDER BY s.date, s.symbol"
    ])
}

export function buildCountSQL(p: Preset | string) {
    const base = typeof p === "string" ? p : buildBacktestSQL(p)

    return joinSQL([
        "SELECT COUNT(*) AS cnt",
        "FROM (",
        base,
        ") t"
    ])
}

export function buildExplainSQL(p: Preset | string) {
    return `EXPLAIN ${typeof p === "string" ? p : buildBacktestSQL(p)}`
}

export function buildBacktestSQL_v2(p: Preset) {
    if (p.type === "sql") {
        return p.sql
    }
    
    const plan = buildPlan(p)
    const optimized = optimizePlan(plan)
    return compileSQL(optimized)
}

export function buildExplainSQL_v2(p: Preset | string) {
    return `EXPLAIN ${typeof p === "string" ? p : buildBacktestSQL_v2(p)}`
}

export function buildCountSQL_v2(p: Preset | string) {
    const base = typeof p === "string" ? p : buildBacktestSQL_v2(p)

    return joinSQL([
        "SELECT COUNT(*) as cnt",
        "FROM (",
        base,
        ") t"
    ])
}