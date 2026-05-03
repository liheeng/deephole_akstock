import React from "react"
import CodeOutlined from '@mui/icons-material/CodeOutlined'
import { type AppModule } from "../../core/moduleRegistry"
import SqlExecutor from "./SqlExecutor"

export const sqlExecutorModule: AppModule = {
    name: "sql_executor",

    menu: [
        {
            text: "SQL Executor",
            icon: React.createElement(CodeOutlined),
            path: "/sql_executor"
        }
    ],

    routes: [
        { path: "/sql_executor", element: React.createElement(SqlExecutor) }
    ]
}