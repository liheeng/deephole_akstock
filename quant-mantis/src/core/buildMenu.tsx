import { modules } from "./moduleRegistry"

export function buildMenu() {
  const map = new Map()

  modules.forEach(m => {
    (m.menu || []).forEach(item => {
      if (!map.has(item.text)) {
        map.set(item.text, { ...item, children: [] })
      }

      const existing = map.get(item.text)

      if (item.children) {
        existing.children.push(...item.children)
      }
    })
  })

  return Array.from(map.values())
}