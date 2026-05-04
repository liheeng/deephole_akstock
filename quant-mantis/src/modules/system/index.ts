import React from "react"
import DescriptionIcon from '@mui/icons-material/Description';
import { type AppModule } from "../../core/moduleRegistry"
import ViewSysLogPage from "./ViewSysLogPage"

export const systemModule: AppModule = {
    name: "system",

    menu: [
        {
            text: "系统日志",
            icon: React.createElement(DescriptionIcon),
            path: "/system_log"
        }
    ],

    routes: [
        { path: "/system_log", element: React.createElement(ViewSysLogPage) }
    ]
}