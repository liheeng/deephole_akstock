import React from "react"
// import StorageOutlined from '@mui/icons-material/StorageOutlined'
import DownloadOutlined from '@mui/icons-material/DownloadOutlined'
import { type AppModule } from "../../core/moduleRegistry"
import ExportDataPage from "./ExportDataPage"

export const exportDataModule: AppModule = {
    name: "export_data",

    menu: [
        {
            text: "Export Data",
            icon: React.createElement(DownloadOutlined),
            path: "/export_data"
        }
    ],

    routes: [
        { path: "/export_data", element: React.createElement(ExportDataPage) }
    ]
}