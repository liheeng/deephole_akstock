import { lazy } from 'react'
import Loadable from 'components/Loadable'

const TaskList = Loadable(lazy(() => import('pages/task/TaskList')))
const TaskDetail = Loadable(lazy(() => import('pages/task/TaskDetail')))
const TaskEditor = Loadable(lazy(() => import('pages/task/TaskEditor')))

const TaskRoutes = {
  path: '/task',
  children: [
    {
      path: '',
      element: <TaskList />
    },
    {
      path: ':id',
      element: <TaskDetail />
    },
    {
      path: ':id/edit',
      element: <TaskEditor />
    }
  ]
}

export default TaskRoutes