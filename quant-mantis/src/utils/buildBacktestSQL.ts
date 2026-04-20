// utils/buildBacktestSQL.ts

type Preset = {
    markets?: string[]
    symbols?: string[]
    sectors?: string[]
    universe?: string
    start: string
    end: string
}

export function buildBacktestSQL(p: Preset) {

    const where: string[] = []
    const joins: string[] = []

    // =========================
    // 1. Base
    // =========================
    let from = "stock_daily s"

    // =========================
    // 2. Universe（最强过滤）
    // =========================
    if (p.universe) {
        joins.push(`
      JOIN universe_map u
        ON s.symbol = u.symbol
        AND u.universe = '${p.universe}'
        AND (u.end_date IS NULL OR s.date <= u.end_date)
        AND s.date >= u.start_date
    `)
    }

    // =========================
    // 3. Sector
    // =========================
    if (p.sectors && p.sectors.length) {

        // 👉 去掉 SEC_ 前缀
        const sectorValues = p.sectors.map(s =>
            `'${s.replace("SEC_", "").toLowerCase()}'`
        )

        joins.push(`
      JOIN stock_sector ss
        ON s.symbol = ss.symbol
        AND ss.sector IN (${sectorValues.join(",")})
        AND (ss.end_date IS NULL OR s.date <= ss.end_date)
        AND s.date >= ss.start_date
    `)
    }

    // =========================
    // 4. Market
    // =========================
    if (p.markets && p.markets.length) {
        const v = p.markets.map(m => `'${m}'`).join(",")
        where.push(`s.market IN (${v})`)
    }

    // =========================
    // 5. Symbol
    // =========================
    if (p.symbols && p.symbols.length) {
        const v = p.symbols.map(s => `'${s}'`).join(",")
        where.push(`s.symbol IN (${v})`)
    }

    // =========================
    // 6. Date（必须）
    // =========================
    where.push(`s.date >= '${p.start}'`)
    where.push(`s.date <= '${p.end}'`)

    // =========================
    // 7. Final SQL
    // =========================
    const sql = `
    SELECT
      s.*
    FROM ${from}

    ${joins.join("\n")}

    WHERE ${where.join("\n AND ")}

    ORDER BY s.date, s.symbol
  `

    return sql
}