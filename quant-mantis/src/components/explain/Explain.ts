import { asNumber } from "../../utils/Strings"

export function getNodeStyle(type?: string, rows?: number) {

    const r = rows || 0
    
    if (type === "PROJECTION") return { bgcolor: "#041d6e", color: "#fff" }

    if (type === "SEQ_SCAN") {
        if (asNumber(r)?? 0 > 5_000_000)
            return { bgcolor: "#d32f2f", color: "#fff" }
        
        if (asNumber(r)?? 0 > 1_000_000) 
            return { bgcolor: "#ed6c02", color: "#fff" }
        return {}
    }

    if (type?.includes("JOIN")) {
        return { bgcolor: "#f57c00", color: "#fff" }
    }

    if (type === "ORDER_BY" && (asNumber(r) ?? 0) > 1_000_000) {
        return { bgcolor: "#ffa000", color: "#000" }
    }

    if (type === "FILTER") return { bgcolor: "#03981c", color: "#fff" }

    return {}
}