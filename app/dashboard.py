# app/dashboard.py

# import streamlit as st
# import duckdb
# from db.db_common import DB

# con = duckdb.connect(DB)

# st.title("📊 Task Dashboard")

# df = con.execute("""
#     SELECT *
#     FROM task_log
#     ORDER BY start_time DESC
#     LIMIT 50
# """).df()

# st.dataframe(df)

import streamlit as st
import requests
import time
from utils.common import is_running_in_docker
from core.job import JobType

API = "http://akstock_api_service:8000" if is_running_in_docker() else "http://localhost:8000"

st.set_page_config(layout="wide")

# 🎯 左侧菜单
menu = st.sidebar.radio(
    "菜单",
    ["Tasks", "Sync CN Daily", "Sync HK Daily", "Sync US Daily", "Logs", "WebConsole"]
)

st.title("📊 Stock Data Dashboard")

# ----------------------------
# 🧩 Tasks 页面
# ----------------------------
if menu == "Tasks":
    st.header("任务列表")

    res = requests.get(f"{API}/tasks")
    data = res.json()

    st.json(data)


# # ----------------------------
# # 🧩 Status 页面
# # ----------------------------
# elif menu == "Status":
#     st.header("当前状态")

#     res = requests.get(f"{API}/status")
#     data = res.json()

#     st.json(data)


# # ----------------------------
# # 🧩 Fetch 页面
# # ----------------------------
# elif menu == "Fetch":
#     st.header("触发数据抓取")

#     if st.button("🚀 执行 Fetch"):
#         res = requests.post(f"{API}/fetch")
#         data = res.json()

#         st.success("任务已触发")
#         st.json(data)

# ----------------------------
# 🧩 Sync CN Daily 页面
# ----------------------------
elif menu == "Sync CN Daily":
    st.header("同步中国A股市场日线数据")

    if st.button("🚀 执行 Sync"):
        res = requests.get(f"{API}/sync_daily/" + JobType.CN_DAILY_SYNC.value)
        data = res.json()

        st.success("任务已触发")
        st.json(data)

# ----------------------------
# 🧩 Sync HK Daily 页面
# ----------------------------
elif menu == "Sync HK Daily":
    st.header("同步香港股市场日线数据")

    if st.button("🚀 执行 Sync"):
        res = requests.get(f"{API}/sync_daily/" + JobType.HK_DAILY_SYNC.value)
        data = res.json()

        st.success("任务已触发")
        st.json(data)

# ----------------------------
# 🧩 Sync US Daily 页面
# ----------------------------
elif menu == "Sync US Daily":
    st.header("同步美国股市场日线数据")

    if st.button("🚀 执行 Sync"):
        res = requests.get(f"{API}/sync_daily/" + JobType.US_DAILY_SYNC.value)
        data = res.json()

        st.success("任务已触发")
        st.json(data)

# ----------------------------
# 🧩 Logs 页面（重点🔥）
# ----------------------------
elif menu == "Logs":
    st.header("日志")

    if st.button("查看日志"):

        placeholder = st.empty()

        while True:
            res = requests.get(f"{API}/logs/tail")
            logs = res.json().get("logs", [])

            placeholder.text("".join(logs))

            time.sleep(5)

elif menu == "WebConsole":
    st.header("Web Console")

    container = st.selectbox(
        "选择容器",
        ["akstock_stock_fetcher", "akstock_api_service"]
    )

    url = f"{API}/terminal/index.html?c={container}"

    st.components.v1.iframe(url, height=600)