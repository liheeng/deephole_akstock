# app/dashboard.py
import os
import io
import streamlit as st
import requests
import time
from sources.data_source import DataSourceApiName
from utils.common import is_running_in_docker
from core.job import JobType

API_SERVICE_NAME = os.getenv("API_SERVICE_NAME", "akstock_api_service")
API_PORT = os.getenv("API_PORT", "8000")
STOCK_FETCHER_SERVICE_NAME = os.getenv("STOCK_FETCHER_SERVICE_NAME", "akstock_stock_fetcher")

API = "http://" + API_SERVICE_NAME + ":" + API_PORT if is_running_in_docker() else "http://localhost:" + API_PORT

st.set_page_config(layout="wide")

# 🎯 左侧菜单 - 新增 Export Data 选项
menu = st.sidebar.radio(
    "菜单",
    ["Tasks", "Sync CN Daily", "Sync HK Daily", "Sync US Daily", "Export Data", "Logs", "WebConsole"],
    key="main_menu"  # 加唯一key避免缓存
)

st.title("📊 Stock Data Dashboard")


# 触发同步函数（修复按钮缓存问题）
def trigger_sync(st, job_type: JobType, data_source_api: str, page_key: str):
    # 给按钮加页面专属key，切换页面后自动重置
    if st.button("🚀 执行同步", key=f"sync_btn_{page_key}"):
        with st.spinner("正在触发任务..."):
            try:
                res = requests.get(
                    url=f"{API}/sync_daily/{job_type.value}",
                    params={"data_source_api": data_source_api}
                )
                if res.status_code == 200:
                    st.success(f"✅ 任务触发成功 | 数据源：{data_source_api}")
                    st.json(res.json())
                else:
                    st.error(f"❌ 任务失败：{res.text}")
            except Exception as e:
                st.error(f"❌ 请求异常：{str(e)}")


def show_export_page(st):
    st.subheader("📤 股票数据导出（超大文件安全版）")

    ALL_COLS = [
        "symbol", "symbol_name", "market", "date",
        "open", "high", "low", "close", "volume", "amount",
        "pct", "turnover", "adjust_mode", "adjust_factor"
    ]

    selected_cols = st.multiselect("选择字段", ALL_COLS, default=ALL_COLS)
    where_sql = st.text_input("WHERE 条件", placeholder="market='CN' AND date>='2025-01-01'")
    export_format = st.radio("导出格式", ["csv", "parquet"], horizontal=True)

    if st.button("🚀 开始导出"):
        if not selected_cols:
            st.warning("请至少选择一个字段！")
            return

        with st.spinner("正在获取数据..."):
            try:
                # ======================
                # 调用 API（内部端口）
                # ======================
                resp = requests.post(
                    f"{API}/export/stream",
                    json={
                        "columns": selected_cols,
                        "where_sql": where_sql,
                        "export_format": export_format
                    },
                    stream=True
                )

                if resp.status_code != 200:
                    st.error(f"导出失败：{resp.text}")
                    return

                # ======================
                # 关键：流式写入 BytesIO（不爆内存）
                # ======================
                buffer = io.BytesIO()
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    buffer.write(chunk)
                buffer.seek(0)

                # ======================
                # 提供正常下载（绝对不报错）
                # ======================
                filename = f"stock_daily.{export_format}"
                st.success(f"✅ 导出完成！共 {len(buffer.getvalue()) / 1024 / 1024:.2f} MB")

                st.download_button(
                    label=f"📥 下载 {filename}",
                    data=buffer,
                    file_name=filename
                )

            except Exception as e:
                st.error(f"错误：{str(e)}")


# ----------------------------
# 🧩 Tasks 页面（修复所有问题版）
# ----------------------------
if menu == "Tasks":
    st.header("任务列表")
    
    # 侧边栏：筛选 + 刷新配置
    st.sidebar.markdown("---")
    st.sidebar.caption("🔍 任务筛选")
    
    # 所有可选状态（严格匹配TaskStatus枚举）
    all_status = [
        "ALL", "CREATED", "SUBMITTED", "RUNNING", 
        "SUSPENDED", "SUCCESS", "FAILED", "PARTIAL_SUCCESS"
    ]
    selected_status = st.sidebar.selectbox(
        "按任务状态筛选",
        all_status,
        index=0,
        key="task_status_filter"
    )

    # 自动刷新配置
    refresh_interval = st.sidebar.number_input(
        "自动刷新间隔（秒）",
        min_value=1, max_value=60, value=5, step=1,
        key="task_refresh_interval"
    )
    auto_refresh = st.sidebar.checkbox(
        "开启自动刷新",
        value=True,
        key="auto_refresh_tasks"
    )        
    
    if st.button("🔄 刷新", key="tasks_btn"):
        st.rerun()

    task_placeholder = st.empty()
    while True:
        if task_placeholder:
            task_placeholder.empty()
            
        task_placeholder = st.empty()
        with task_placeholder.container():
            try:
                resp = requests.get(f"{API}/tasks")
                tasks = resp.json() if resp.status_code == 200 else []
            except Exception as e:
                st.error(f"获取任务失败: {str(e)}")
                tasks = []

            # ✅ 修复问题1：严格过滤，只保留符合条件的任务
            filtered_tasks = []
            if selected_status == "ALL":
                filtered_tasks = tasks
            else:
                for t in tasks:
                    task_status = t.get("status", "").strip().upper()
                    if task_status == selected_status:
                        filtered_tasks.append(t)

            if not filtered_tasks:
                st.warning(f"暂无【{selected_status}】状态的任务")
            else:
                st.success(f"共找到 {len(filtered_tasks)} 个任务 (筛选：{selected_status})")
                
                for task in filtered_tasks:
                    task_id = task["id"]
                    status = task["status"].strip().upper()
                    desc = task["description"] or "无描述"

                    # 任务状态图标匹配
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

                    # 任务主展开项
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
                            if not jobs:
                                st.info("无 Job")
                            else:
                                for job in jobs:
                                    job_id = job.get("id", "unknown")
                                    job_name = job.get("name", f"Job-{job_id}")
                                    job_type = job.get("type", job.get("job_type", "N/A"))
                                    job_status = job.get("status", "N/A").strip().upper()

                                    # Job 状态图标（严格匹配JobStatus枚举）
                                    if job_status == "CREATED":
                                        j_icon = "⚪"
                                        j_text = "已创建"
                                    elif job_status == "QUEUED":
                                        j_icon = "🔵"
                                        j_text = "排队中"
                                    elif job_status == "RUNNING":
                                        j_icon = "🟡"
                                        j_text = "运行中"
                                    elif job_status == "SUCCESS":
                                        j_icon = "🟢"
                                        j_text = "成功"
                                    elif job_status == "FAILED":
                                        j_icon = "🔴"
                                        j_text = "失败"
                                    else:
                                        j_icon = "⚫"
                                        j_text = job_status

                                    # ✅ 修复问题3：状态文案移到图标后，顺序：图标+状态 | 名称 | 类型
                                    with st.expander(f"{j_icon} {j_text} | {job_name} | {job_type}"):
                                        st.markdown("#### 📄 Job 详情")
                                        st.write(f"ID: `{job.get('id')}`")
                                        st.write(f"状态: **{job.get('status')}**")
                                        st.write(f"类型: {job.get('job_type', 'N/A')}")
                                        st.write(f"开始时间: {job.get('start_time', 'N/A')}")
                                        st.write(f"结束时间: {job.get('stop_time', 'N/A')}")
                                        st.write(f"消息: {job.get('message', 'N/A')}")
                        st.divider()

        if not auto_refresh:
            break
        time.sleep(refresh_interval)
        

# ----------------------------
# 🧩 Sync CN Daily 页面（修复按钮缓存）
# ----------------------------
elif menu == "Sync CN Daily":
    st.header("同步中国A股市场日线数据")
    data_source = st.selectbox(
        "选择数据源",
        [DataSourceApiName.IFIND_API.value, DataSourceApiName.AKSHARE_SINA_API.value],
        index=0,
        help="选择要同步数据的数据源",
        key="cn_data_source"
    )
    # ✅ 修复问题2：加页面专属key，切换页面后按钮自动重置
    trigger_sync(st, JobType.CN_DAILY_SYNC, data_source, page_key="cn")


# ----------------------------
# 🧩 Sync HK Daily 页面（修复按钮缓存）
# ----------------------------
elif menu == "Sync HK Daily":
    st.header("同步香港股市场日线数据")
    data_source = st.selectbox(
        "选择数据源",
        [DataSourceApiName.IFIND_API.value, DataSourceApiName.AKSHARE_SINA_API.value],
        index=0,
        help="选择要同步数据的数据源",
        key="hk_data_source"
    )
    trigger_sync(st, JobType.HK_DAILY_SYNC, data_source, page_key="hk")


# ----------------------------
# 🧩 Sync US Daily 页面（修复按钮缓存）
# ----------------------------
elif menu == "Sync US Daily":
    st.header("同步美国股市场日线数据")
    data_source = st.selectbox(
        "选择数据源",
        [DataSourceApiName.IFIND_API.value, DataSourceApiName.AKSHARE_SINA_API.value],
        index=0,
        help="选择要同步数据的数据源",
        key="us_data_source"
    )
    trigger_sync(st, JobType.US_DAILY_SYNC, data_source, page_key="us")

# ----------------------------
# 🧩 Export Data 页面（新增）
# ----------------------------
elif menu == "Export Data":
    show_export_page(st)

# ----------------------------
# 🧩 Logs 页面
# ----------------------------
elif menu == "Logs":
    st.header("日志")
    if st.button("查看日志", key="log_btn"):
        placeholder = st.empty()
        while True:
            res = requests.get(f"{API}/logs/tail")
            logs = res.json().get("logs", [])
            placeholder.text("".join(logs))
            time.sleep(5)


# ----------------------------
# 🧩 WebConsole 页面
# ----------------------------
elif menu == "WebConsole":
    st.header("Web Console")
    container = st.selectbox(
        "选择容器",
        [STOCK_FETCHER_SERVICE_NAME, API_SERVICE_NAME],
        key="container_select"
    )
    url = f"{API}/terminal/index.html?c={container}"
    st.components.v1.iframe(url, height=600)