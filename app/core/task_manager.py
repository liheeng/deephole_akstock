# app/core/task_manager.py

from datetime import datetime
from utils.log_manager import get_default_logger
from core.task import Task, TaskMode, TaskStatus
from core.job import JOB_DEFINITIONS, Job, JobStatus, JobType, JobDefinition
from db.db_common import DB, safe_time
import json
from db.duckdb import DuckDBController

db = DuckDBController(DB)

def job_dag_ok(job: Job) -> bool:
    if not job.depends_on:
        return True

    rows = db.execute("""
        SELECT COUNT(*)
        FROM jobs
        WHERE id IN ?
        AND status != 'SUCCESS'
    """, [tuple(job.depends_on)], fetch="one")

    return rows[0] == 0

def job_concurrency_ok(job: Job) -> bool:
    defn = JOB_DEFINITIONS[job.type]

    if defn.max_concurrency == 0:
        return True

    row = db.execute("""
        SELECT COUNT(*)
        FROM running_jobs
        WHERE concurrency_key = ?
    """, [defn.concurrency_key], fetch="one")

    return row[0] < defn.max_concurrency

def job_singleton_ok(job: Job) -> bool:
    defn = JOB_DEFINITIONS[job.type]

    if not defn.singleton:
        return True

    row = db.execute("""
        SELECT COUNT(*)
        FROM jobs
        WHERE type = ?
        AND status IN ('CREATED','QUEUED','RUNNING')
    """, [job.type.value], fetch="one")

    return row[0] == 0

def job_retry_ok(job: Job) -> bool:
    return job.retry_count < job.retries


def job_can_run(job: Job) -> bool:
    return (
        job_dag_ok(job)
        and job_concurrency_ok(job)
        and job_retry_ok(job)
    )

def job_can_create(job):
    return job_singleton_ok(job)

def task_is_allowed(task: Task) -> bool:
    for job in task.jobs:
        if not job_can_create(job):
            return False
    return True

def task_can_run(task: Task) -> bool:

    for job in task.jobs:
        if not job_can_run(job):
            return False

    return True

def build_task(task_row: list, job_rows: list) -> Task | None:
    if not task_row:
        return None
    
    task = Task(
        id=task_row[0],
        description=task_row[1],
        status=TaskStatus(task_row[2]),
        mode=TaskMode(task_row[3]),
        start_time=task_row[4],
        execute_time=task_row[5],
        stop_time=task_row[6],
        message=task_row[7],
        create_time=task_row[8]
    )

    jobs = []

    for row in job_rows:
    
        # If the job belongs to a different task, log a error and skip it
        if row[1] != task.id:
            print(f"Error: job {row[0]} belongs to task {row[1]}, expected {task.id}")
            get_default_logger().error(f"Job {row[0]} belongs to task {row[1]}, expected {task.id}")
            continue

        job = Job(
            id=row[0],
            type=JobType(row[1]),
            status=JobStatus(row[2]),
            task_id=row[3],
            task = task,  # type: ignore
            params=json.loads(row[4] or "{}"),
            depends_on=json.loads(row[5] or "[]"),
            retry_count=row[6],
            retries=row[7],
            execute_time=row[8],
            stop_time=row[9],
            message=row[10],
            error=row[11]
        )

        jobs.append(job)

    task.jobs = jobs
    return task

class TaskManager:
    _instance = None

    def __new__(cls):
        # 单例模式，保证全局只有一个task manager
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def load_save_task(self, task: Task) -> Task:
        """load task from DB if exists otherwise save it into DB

        Args:
            task (Task): _description_
        """
        _task = self.load_task(task.id)
        if not _task:
            self.save_task(task)
            return task
        else:
            return _task

    def save_task(self, task: Task):
        db.execute(
        """
        INSERT INTO tasks (
            id, description, status, mode,
            start_time, execute_time, stop_time, message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        params=[
            task.id,
            task.description,
            task.status.value,
            task.mode.value,
            safe_time(task.start_time),
            safe_time(task.execute_time),
            safe_time(task.stop_time),
            task.message
        ])

        for job in task.jobs:
            self.save_job(job)

    def save_job(self, job: Job):
        db.execute("""
            INSERT INTO jobs (
                id, type, status, task_id,
                params, depends_on,
                retries, retry_count,
                execute_time, stop_time,
                message, error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        params=[
            job.id,
            job.type.value,    
            job.status.value,
            job.task_id,

            json.dumps(job.depends_on),
            json.dumps(job.params),

            job.retries,
            job.retry_count,

            safe_time(job.execute_time),
            safe_time(job.stop_time),

            job.message,
            job.error
        ])

    def load_jobs_by_task_id(self, task_id: str):

        def callback(con):

            job_rows = con.execute("""
                SELECT * FROM jobs WHERE task_id=?
            """, (task_id,)).fetchall()

            return job_rows

        job_rows = db.execute("SELECT 1", callback=callback)

        jobs = []
        for row in job_rows:
            job = Job(
                id=row[0],
                type=JobType(row[1]),
                status=JobStatus(row[2]),
                task_id=row[3],
                params=json.loads(row[4] or "{}"),
                depends_on=json.loads(row[5] or "[]"),
                retry_count=row[6],
                retries=row[7],
                execute_time=row[8],
                stop_time=row[9],
                message=row[10],
                error=row[11]
            )

            jobs.append(job)

        return jobs
    
    def load_task(self, task_id: str):

        def callback(con):

            task_row = con.execute("""
                SELECT * FROM tasks WHERE id=?
            """, (task_id,)).fetchone()

            job_rows = con.execute("""
                SELECT * FROM jobs WHERE task_id=?
            """, (task_id,)).fetchall()

            return task_row, job_rows

        task_row, job_rows = db.execute("SELECT 1", callback=callback)

        return build_task(task_row, job_rows) # type: ignore

    def update_job_status_by_id(self, job_id: str, new_status: JobStatus, message="", error="") -> datetime:
        _time = datetime.now()
        if new_status in [JobStatus.SUCCESS, JobStatus.FAILED]:
            db.execute("""
                UPDATE jobs
                SET status=?, message=?, error=?, stop_time=?, update_time=now()
                WHERE id=?
            """, 
            params = [
                new_status.value,
                message,
                error,
                _time.strftime("%Y-%m-%d %H:%M:%S"),
                job_id
            ])
        elif new_status == JobStatus.RUNNING:
            db.execute("""
                UPDATE jobs
                SET status=?, message=?, error=?, execute_time=?, update_time=now()
                WHERE id=?
            """, 
            params = [
                new_status.value,
                message,
                error,
                _time.strftime("%Y-%m-%d %H:%M:%S"),
                job_id
            ])
        elif new_status == JobStatus.QUEUED:
            db.execute("""
                UPDATE jobs
                SET status=?, message=?, error=?, update_time=now()
                WHERE id=?
            """, 
            params = [
                new_status.value,
                message,
                error,
                job_id
            ])
        else:
            get_default_logger().warning(f"Unsupported job status to update: {new_status} of job {job_id}")
        
        return _time
    
    def update_job_status(self, job: Job, new_status: JobStatus) -> JobStatus:
        status = job.status
        task_status_changed = job.update_status(new_status)
        _time = self.update_job_status_by_id(job_id = job.id, new_status=job.status, message=job.message, error=job.error).strftime("%Y-%m-%d %H:%M:%S")
        if job.status in [JobStatus.SUCCESS, JobStatus.FAILED]:
            job.stop_time = _time
        elif job.status == JobStatus.RUNNING:
            job.execute_time = _time
        
        if task_status_changed and job.task:
            task_manager.update_task_status(job.task, job.task.status)

        return status

    def update_task_status_by_id(self, task_id: str, new_status: TaskStatus, message="") -> datetime:
        _time = datetime.now()
        if new_status in [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.PARTIAL_SUCCESS]:
            db.execute("""
                UPDATE tasks
                SET status=?, stop_time=?, message=?, update_time=now()
                WHERE id=?
            """, 
            params = [
                new_status.value,
                _time.strftime("%Y-%m-%d %H:%M:%S"),
                message,
                task_id
            ])
        elif new_status == TaskStatus.SUBMITTED:
            db.execute("""
                UPDATE tasks
                SET status=?, start_time=?, message=?, update_time=now()
                WHERE id=?
            """, 
            params = [
                new_status.value,
                _time.strftime("%Y-%m-%d %H:%M:%S"),
                message,
                task_id
            ])
        elif new_status == TaskStatus.RUNNING:
            db.execute("""
                UPDATE tasks
                SET status=?, execute_time=?, message=?, update_time=now()
                WHERE id=?
            """, 
            params = [
                new_status.value,
                _time.strftime("%Y-%m-%d %H:%M:%S"),
                message,
                task_id
            ])
        else:
            get_default_logger().warning(f"Unsupported task status to update: {new_status} of task {task_id}")
        return _time

    def update_task_status(self, task: Task, new_status: TaskStatus, message="") -> TaskStatus:
        status = task.status
        task.status = new_status
        _time = self.update_task_status_by_id(task.id, task.status, task.message).strftime("%Y-%m-%d %H:%M:%S")
        if task.status in [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.PARTIAL_SUCCESS]:
            task.stop_time = _time
        elif task.status == TaskStatus.SUBMITTED:
            task.start_time = _time
        elif task.status == TaskStatus.RUNNING:
            task.execute_time = _time
        return status
    
    def list_tasks(self, limit=20):    
        def callback(con):

            task_rows = con.execute("""
                SELECT * FROM tasks
            ORDER BY create_time DESC
            LIMIT ?
            """, (limit,)).fetchall()

            task_ids = [row[0] for row in task_rows]
            job_rows = con.execute("""
                SELECT * FROM jobs WHERE task_id in ?
            """, (tuple(task_ids),)).fetchall()

            return task_rows, job_rows

        task_rows, job_rows = db.execute("SELECT 1", callback=callback)

        tasks = []
        for task_row in task_rows:
            tasks.append(build_task(task_row, job_rows))
        return tasks
    
    def clean_stale_running_jobs(self):
        db.execute("""
            DELETE FROM running_jobs
            WHERE start_time < now() - INTERVAL 12 HOUR
        """)

    def try_acquire_concurrency_slot(self, job: Job, defn: JobDefinition):

        if defn.max_concurrency == 0:
            return True

        row = db.execute("""
            SELECT COUNT(*)
            FROM running_jobs
            WHERE concurrency_key = ?
        """, [defn.concurrency_key], fetch="one")

        if row[0] >= defn.max_concurrency:
            return False

        # 占用槽位
        db.execute("""
            INSERT INTO running_jobs (job_id, task_id, type, concurrency_key, start_time)
            VALUES (?, ?, ?, ?, now())
        """, [job.id, job.task_id, job.type.value, defn.concurrency_key])

        return True

    def release_concurrency_slot(self, job: Job):
        db.execute("""
            DELETE FROM running_jobs
            WHERE job_id = ?
        """, [job.id])

task_manager = TaskManager()