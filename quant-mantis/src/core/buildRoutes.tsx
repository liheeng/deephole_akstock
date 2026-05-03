import { Routes, Route } from "react-router-dom"
import { modules } from "./moduleRegistry"

export function BuildRoutes() {
  return (
    <Routes>
      {modules.flatMap(m =>
        (m.routes || []).map(r => (
          <Route key={r.path} path={r.path} element={r.element} />
        ))
      )}
    </Routes>
  )
}