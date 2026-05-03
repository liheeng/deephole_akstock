import { ReactNode } from "react"

import { taskModule } from "modules/task"
import { scriptModule } from "modules/script"
import { backtestModule } from "modules/backtest"
import { jupyterModule } from "modules/jupyter"

export const modules = [
  taskModule,
  scriptModule,
  backtestModule,
  jupyterModule
]

export interface AppModule {
  name: string

  menu?: MenuItem[]

  routes?: RouteItem[]
}

export interface MenuItem {
  text: string
  path?: string
  icon?: ReactNode
  children?: MenuItem[]
}

export interface RouteItem {
  path: string
  element: ReactNode
}