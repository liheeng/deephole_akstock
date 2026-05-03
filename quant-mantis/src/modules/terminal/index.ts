import React from "react"
import TerminalIcon from "@mui/icons-material/Terminal";
import { type AppModule } from "../../core/moduleRegistry"
import TerminalPage from "./TerminalPage"

export const terminalModule: AppModule = {
    name: "terminal",

    menu: [
        {
            text: "终端",
            icon: React.createElement(TerminalIcon),
            path: "/terminal"
        }
    ],

    routes: [
        { path: "/terminal", element: React.createElement(TerminalPage) }
    ]
}