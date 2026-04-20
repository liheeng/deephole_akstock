import { DataGrid, GridToolbar } from '@mui/x-data-grid';
import type { DataGridProps } from '@mui/x-data-grid'

// 扩展 Props：完全继承原生 DataGrid 所有属性 + 新增自定义属性
export interface UniDataGridProps extends DataGridProps {
  /** 单元格基础背景色 */
  cellBgColor?: string;
  /** 斑马条纹背景色 */
  stripeBgColor?: string;
  /** 是否启用斑马条纹 */
  striped?: boolean;
}

export default function UniDataGrid({
  // 自定义参数
  cellBgColor = '#5f5d5d',
  stripeBgColor = 'rgb(71, 70, 70)',
  striped = true,

  // 原生所有属性
  slots,
  sx,
  rows,
  ...props
}: UniDataGridProps) {
  return (
    <DataGrid
      // 原生属性透传 100% 支持
      {...props}
      // ✅ 强制自动生成 ID（永远不报错）
      rows={rows.map((row, idx) => ({ id: idx, ...row }))}
      // 合并插槽：保留用户传入的 slots + 默认自带 toolbar
      slots={{
        toolbar: GridToolbar,
        ...slots,
      }}
      // 合并样式：自定义斑马条样式 + 外部 sx
      sx={[
        {
          // 表体单元格
          '& .MuiDataGrid-cell': {
            backgroundColor: cellBgColor,
          },
          // 斑马条纹（受控）
          ...(striped && {
            '& .MuiDataGrid-row:nth-of-type(even) > .MuiDataGrid-cell': {
              backgroundColor: stripeBgColor,
            },
          }),
          // 表头保持原生样式不变
          '& .MuiDataGrid-columnHeaders': {
            backgroundColor: 'inherit',
          },
          border: 'none',
        },
        // 外部 sx 优先级更高
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
    />
  );
}