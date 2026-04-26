// src/menu-items/stock.ts
import { 
  AssignmentOutlined, 
  SyncAltOutlined, 
  CloudDownloadOutlined, 
  StorageOutlined
} from '@mui/icons-material';

const stockMenu = {
  id: 'stock-management',
  title: '股票系统',
  type: 'group',
  children: [
    {
      id: 'tasks',
      title: '任务列表',
      type: 'item',
      url: '/tasks',
      icon: AssignmentOutlined
    },
    {
      id: 'sync',
      title: '数据同步',
      type: 'collapse',
      icon: SyncAltOutlined,
      children: [
        { id: 'sync-cn', title: 'A股同步', type: 'item', url: '/sync/cn' },
        { id: 'sync-hk', title: '港股同步', type: 'item', url: '/sync/hk' },
        { id: 'sync-us', title: '美股同步', type: 'item', url: '/sync/us' }
      ]
    },
    {
      id: 'sql',
      title: 'SQL 执行器',
      type: 'item',
      url: '/sql-executor',
      icon: StorageOutlined
    },
    {
      id: 'export',
      title: '导出数据',
      type: 'item',
      url: '/export',
      icon: CloudDownloadOutlined
    }
  ]
};

export default stockMenu;