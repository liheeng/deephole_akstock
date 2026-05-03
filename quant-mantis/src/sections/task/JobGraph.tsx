import ReactFlow from "reactflow"

export default function JobGraph({ jobs }: any) {
  const nodes = jobs.map((j: any) => ({
    id: j.id,
    data: { label: j.type },
    position: { x: 100, y: 100 }
  }))

  const edges = jobs.flatMap((j: any) =>
    j.depends_on.map((d: string) => ({
      id: `${d}-${j.id}`,
      source: d,
      target: j.id
    }))
  )

  return (
    <div style={{ height: 400 }}>
      <ReactFlow nodes={nodes} edges={edges} />
    </div>
  )
}