import { type ReactNode } from "react"

import { backtestModule } from "../modules/backtest"
import { JutyperLabModule } from "../modules/jupyter"
import { sqlExecutorModule } from "../modules/sql"
import { exportDataModule } from "../modules/exportor"
import { taskModule } from "../modules/task"
import { terminalModule } from "../modules/terminal"
import { systemModule } from '../modules/system'

export const modules = [
    backtestModule,
    JutyperLabModule,
    sqlExecutorModule,
    exportDataModule,
    taskModule,
    terminalModule,
    systemModule
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