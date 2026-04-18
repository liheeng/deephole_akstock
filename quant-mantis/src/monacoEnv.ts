// monacoEnv.ts
import * as monaco from "monaco-editor"

let initialized = false

export function initMonacoEnv() {
  if (initialized) return monaco

  // 👇 Vite 必须这样配置 worker
  self.MonacoEnvironment = {
    getWorker(_: any, label: string) {
      if (label === "json") {
        return new Worker(
          new URL(
            "monaco-editor/esm/vs/language/json/json.worker",
            import.meta.url
          ),
          { type: "module" }
        )
      }
      if (label === "css") {
        return new Worker(
          new URL(
            "monaco-editor/esm/vs/language/css/css.worker",
            import.meta.url
          ),
          { type: "module" }
        )
      }
      if (label === "html") {
        return new Worker(
          new URL(
            "monaco-editor/esm/vs/language/html/html.worker",
            import.meta.url
          ),
          { type: "module" }
        )
      }
      if (label === "typescript" || label === "javascript") {
        return new Worker(
          new URL(
            "monaco-editor/esm/vs/language/typescript/ts.worker",
            import.meta.url
          ),
          { type: "module" }
        )
      }

      return new Worker(
        new URL(
          "monaco-editor/esm/vs/editor/editor.worker",
          import.meta.url
        ),
        { type: "module" }
      )
    }
  }

  initialized = true
  return monaco
}