import Editor from "@monaco-editor/react"
import { useEffect } from "react"

export default function DSLMonacoEditor({
  value,
  onChange,
  nodes = []
}: any) {

  const handleMount = (editor: any, monaco: any) => {
    monaco.languages.registerCompletionItemProvider("python", {
      provideCompletionItems: () => {
        return {
          suggestions: nodes.map((n: any) => ({
            label: n.name,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: buildSnippet(n),
            documentation: n.desc
          }))
        }
      }
    })
  }

  const buildSnippet = (node: any) => {
    if (!node.params?.length) return node.name

    const args = node.params.map((p: any, i: number) => {
      return `\${${i + 1}:${p.default || ""}}`
    })

    return `${node.name}(${args.join(", ")})`
  }

  return (
    <Editor
      height="80px"
      defaultLanguage="python"
      value={value}
      onChange={(v) => onChange(v)}
      onMount={handleMount}
      options={{
        minimap: { enabled: false },
        fontSize: 13
      }}
    />
  )
}