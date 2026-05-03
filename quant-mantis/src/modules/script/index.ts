import { AppModule } from "core/moduleRegistry"
import ScriptExecutorPage from "./pages/ScriptExecutorPage"

export const scriptModule: AppModule = {
  name: "script",

  menu: [
    {
      text: "任务中心",
      children: [
        { text: "Script Executor", path: "/script_executor" }
      ]
    }
  ],

  routes: [
    { path: "/script_executor", element: <ScriptExecutorPage /> }
  ]
}