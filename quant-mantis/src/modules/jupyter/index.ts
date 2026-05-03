import React from "react"
import EditDocumentIcon from '@mui/icons-material/EditDocument'
import { type AppModule } from "../../core/moduleRegistry"
import JupyterDebugPage from "./JupyterDebugPage"

export const JutyperLabModule: AppModule = {
    name: "jupyter_lab",

    menu: [
        {
            text: "Jupyter Lab",
            icon: React.createElement(EditDocumentIcon),
            path: "/jupyter_lab"
        }
    ],

    routes: [
        { path: "/jupyter_lab", element: React.createElement(JupyterDebugPage) }
    ]
}