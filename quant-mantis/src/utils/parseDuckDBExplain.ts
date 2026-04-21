export type PlanNode = {
    name: string
    rows?: string
    extra: string[]
    children: PlanNode[]
}

export function parseDuckDBExplain(text: string): PlanNode[] {
    if (!text) return []

    const lines = text.replace(/\\n/g, "\n").split("\n")

    const nodes: PlanNode[] = []
    let current: PlanNode | null = null

    for (const line of lines) {

        // ===== 1️⃣ 匹配节点名 =====
        const nameMatch = line.match(/│\s*([A-Z_]+)\s*│/)
        if (nameMatch) {
            current = {
                name: nameMatch[1],
                children: [],
                extra: []
            }
            nodes.push(current)
            continue
        }

        if (!current) continue

        // ===== 2️⃣ rows =====
        const rowsMatch = line.match(/~([\d,]+) rows/)
        if (rowsMatch) {
            current.rows = rowsMatch[1]
            continue
        }

        // ===== 3️⃣ 额外信息（过滤空行 / 边框）=====
        if (
            line.includes("│") &&
            !line.includes("────") &&
            !line.includes("┌") &&
            !line.includes("└")
        ) {
            const content = line.replace(/│/g, "").trim()
            if (content) current.extra.push(content)
        }
    }

    // ===== 4️⃣ 构建链式树（DuckDB 默认是线性 pipeline）=====
    for (let i = 0; i < nodes.length - 1; i++) {
        nodes[i].children.push(nodes[i + 1])
    }

    return nodes.length ? [nodes[0]] : []
}