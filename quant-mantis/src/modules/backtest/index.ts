import React from "react"
import AnalyticsOutlined from '@mui/icons-material/AnalyticsOutlined'
import { type AppModule } from "../../core/moduleRegistry"
import BacktestPage from "./BacktestPage"
import { Navigate } from "react-router-dom"

export const backtestModule: AppModule = {
    name: "backtest",

    menu: [
        {
            text: "Backtest",
            icon: React.createElement(AnalyticsOutlined),
            path: "/backtest"
        }
    ],

    routes: [
        { path: "/", element: React.createElement(Navigate, { to: "/terminal", replace: true })  },
        { path: "/backtest", element: React.createElement(BacktestPage) }
    ]
}