import Editor from '@monaco-editor/react'

export default function ScriptEditor({ value, onChange }: any) {
  return (
    <Editor
      height="500px"
      defaultLanguage="python"
      value={value}
      onChange={(v) => onChange(v)}
    />
  )
}