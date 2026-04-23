export function joinSQL(lines: string[]) {
    return lines
        .map(l => l.trim())
        .filter(Boolean)
        .join("\n")
}

export function toSQLList(arr: string[]) {
    return Array.from(new Set(arr))
        .map(v => `'${escapeSQL(v)}'`)
        .join(", ")
}

export function escapeSQL(str: string) {
    return str.replace(/'/g, "''")
}