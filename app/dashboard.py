# app/dashboard.py

import streamlit as st
import requests
import time
from sources.data_source import DataSourceApiName
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


# dashboard.py 里的触发函数
def trigger_sync(st, job_type: JobType, data_source_api: str):
    if st.button("🚀 执行同步"):
        with st.spinner("正在触发任务..."):
            try:
                # ✅ 正确：GET 请求用 params 传递查询参数
                res = requests.get(
                    url=f"{API}/sync_daily/{job_type.value}",
                    params={"data_source_api": data_source_api}  # 👈 这是正确方式
                )

                if res.status_code == 200:
                    st.success(f"✅ 任务触发成功 | 数据源：{data_source_api}")
                    st.json(res.json())
                else:
                    st.error(f"❌ 任务失败：{res.text}")
            except Exception as e:
                st.error(f"❌ 请求异常：{str(e)}")


# ----------------------------
# 🧩 Tasks 页面（自动刷新 + 不展开显示完整状态 + 匹配你的枚举）
# ----------------------------
if menu == "Tasks":
    st.header("任务列表")
    
    # 自动刷新配置
    refresh_interval = st.sidebar.number_input("自动刷新间隔（秒）", min_value=1, max_value=60, value=5, step=1, key="task_refresh")
    auto_refresh = st.sidebar.checkbox("开启自动刷新", value=True, key="auto_refresh_tasks")

    # 占位符：无闪烁刷新
    task_placeholder = st.empty()

    while True:
        with task_placeholder.container():
            try:
                resp = requests.get(f"{API}/tasks")
                tasks = resp.json() if resp.status_code == 200 else []
            except Exception as e:
                st.error(f"获取任务失败: {str(e)}")
                tasks = []

            if not tasks:
                st.warning("暂无任务")
            else:
                for task in tasks:
                    task_id = task["id"]
                    status = task["status"].strip().upper()  # 确保匹配枚举
                    desc = task["description"] or "无描述"

                    # ===============================
                    # 🔥 完全匹配你的 TaskStatus 枚举
                    # ===============================
                    if status == "CREATED":
                        status_icon = "⚪"
                        status_text = "已创建"
                    elif status == "SUBMITTED":
                        status_icon = "🔵"
                        status_text = "排队中"
                    elif status == "RUNNING":
                        status_icon = "🟡"
                        status_text = "运行中"
                    elif status == "SUSPENDED":
                        status_icon = "🟤"
                        status_text = "已暂停"
                    elif status == "SUCCESS":
                        status_icon = "🟢"
                        status_text = "成功"
                    elif status == "FAILED":
                        status_icon = "🔴"
                        status_text = "失败"
                    elif status == "PARTIAL_SUCCESS":
                        status_icon = "🟠"
                        status_text = "部分成功"
                    else:
                        status_icon = "⚫"
                        status_text = status

                    # 标题直接显示：图标 + 状态 + ID + 描述
                    # 不展开就能看到！
                    with st.expander(f"{status_icon} {status_text} | ID:{task_id} | {desc}"):
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
                            st.dataframe(jobs, use_container_width=True) if jobs else st.info("无 Job")
                        st.divider()

        if not auto_refresh:
            break
        time.sleep(refresh_interval)

# ----------------------------
# 🧩 Sync CN Daily 页面
# ----------------------------
elif menu == "Sync CN Daily":
    st.header("同步中国A股市场日线数据")
    # 新增数据源选择下拉框
    data_source = st.selectbox(
        "选择数据源",
        [DataSourceApiName.IFIND_API.value, DataSourceApiName.AKSHARE_SINA_API.value],  # 可根据实际支持的数据源扩展
        index=0,  # 默认选中ifind
        help="选择要同步数据的数据源"
    )
    trigger_sync(st, JobType.CN_DAILY_SYNC, data_source)

# ----------------------------
# 🧩 Sync HK Daily 页面
# ----------------------------
elif menu == "Sync HK Daily":
    st.header("同步香港股市场日线数据")
    # 新增数据源选择下拉框
    data_source = st.selectbox(
        "选择数据源",
        [DataSourceApiName.IFIND_API.value, DataSourceApiName.AKSHARE_SINA_API.value],  # 美股可适配专属数据源
        index=0,
        help="选择要同步数据的数据源"
    )
    trigger_sync(st, JobType.HK_DAILY_SYNC, data_source)

# ----------------------------
# 🧩 Sync US Daily 页面
# ----------------------------
elif menu == "Sync US Daily":
    st.header("同步美国股市场日线数据")
    # 新增数据源选择下拉框
    data_source = st.selectbox(
        "选择数据源",
        [DataSourceApiName.IFIND_API.value, DataSourceApiName.AKSHARE_SINA_API.value],  # 美股可适配专属数据源
        index=0,
        help="选择要同步数据的数据源"
    )
    trigger_sync(st, JobType.US_DAILY_SYNC, data_source)

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
