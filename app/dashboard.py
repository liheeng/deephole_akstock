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

    try:
        resp = requests.get(f"{API}/tasks")
        if resp.status_code == 200:
            tasks = resp.json()
        else:
            st.error(f"请求失败：{resp.status_code}")
            tasks = []
    except Exception as e:
        st.error(f"连接 B 服务失败：{str(e)}")
        tasks = []

    if not tasks:
        st.warning("暂无任务")
        st.stop()

    # 展示任务列表
    for task in tasks:
        status = task["status"]
        desc = task["description"] or "无描述"

        # 展开器
        with st.expander(f"✅ {task['id']} | {status} | {desc}"):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### 任务信息")
                st.write(f"ID：`{task['id']}`")
                st.write(f"状态：**{task['status']}**")
                st.write(f"模式：{task['mode']}")
                st.write(f"开始时间：{task['start_time']}")
                st.write(f"完成时间：{task['stop_time']}")
                st.write(f"消息：{task['message']}")

            with col2:
                st.markdown("### 关联 Jobs")
                jobs = task.get("jobs", [])
                if jobs:
                    st.dataframe(jobs, use_container_width=True)
                else:
                    st.info("无 Job")

            st.divider()


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