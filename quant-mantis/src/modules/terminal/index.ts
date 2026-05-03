import { type AppModule } from "../../core/moduleRegistry"
import TerminalPage from "./TerminalPage"

export const terminaltModule: AppModule = {
  name: "script",

  menu: [
    {
      text: "终端",
      children: [
        { text: "Web终端", path: "/terminal" }
      ]
    }
  ],

  routes: [
    { path: "/terminal", element: <TerminalPage /> }
  ]
}