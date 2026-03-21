# app/utils/task_db.py

import duckdb
import time
from db.db_common import DB
from datetime import datetime
import uuid

DB_PATH = DB

def generate_task_id():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"_task_{ts}_{uid}"

def generate_job_id():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"_job_{ts}_{uid}"

def get_conn():
    return duckdb.connect(DB_PATH)


def next_id(con):
    return con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM task_log").fetchone()[0]


def insert_task(task_id, task_name, source):
    con = get_conn()

    id = next_id(con)

    now = time.strftime("%Y-%m-%d %H:%M:%S")

    con.execute("""
        INSERT INTO task_log
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        id,
        task_id,
        task_name,
        source,
        "RUNNING",
        now,
        None,
        None,
        ""
    ))

    con.close()

    return id


def finish_task(id, status, message=""):
    con = get_conn()

    end_time = time.strftime("%Y-%m-%d %H:%M:%S")

    # 计算 duration
    start_time = con.execute(
        "SELECT start_time FROM task_log WHERE id=?",
        (id,)
    ).fetchone()[0]

    duration = con.execute(
        "SELECT epoch(CAST(? AS DATE)) - epoch(CAST(? AS DATE))",
        (end_time, start_time)
    ).fetchone()[0]

    con.execute("""
        UPDATE task_log
        SET status=?, end_time=?, duration=?, message=?
        WHERE id=?
    """, (
        status,
        end_time,
        duration,
        message,
        id
    ))

    con.close()