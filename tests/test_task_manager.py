import unittest
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

from core.task import Task, TaskStatus, TaskMode
from core.job import Job, JobStatus, JobType
from core.task_manager import (
    build_task,
    job_dag_ok,
    job_concurrency_ok,
    job_singleton_ok,
    job_retry_ok,
    job_can_run,
    job_can_create,
    task_is_allowed,
    task_can_run,
    task_manager,
)


class TestBuildTask(unittest.TestCase):

    def test_build_task_empty_row(self):
        self.assertIsNone(build_task(None, []))

    def test_build_task_valid(self):
        task_row = (
            "task_001",
            "test task",
            TaskStatus.CREATED.value,
            TaskMode.DAG.value,
            None, None, None, "",
            "2025-01-01 00:00:00"
        )

        # ✅ 这里字段顺序 100% 匹配你真实的表结构
        job_rows = [
            (
                "job_001",        # 0 id
                JobType.CN_DAILY_SYNC.value,  # 1 type
                "CREATED",        # 2 status
                "task_001",       # 3 task_id  <<< 必须等于 task.id
                "{}",             # 4 params
                "[]",             # 5 depends_on
                0,                # 6 retry_count
                3,                # 7 retries
                None,             # 8 execute_time
                None,             # 9 stop_time
                "",               # 10 message
                ""                # 11 error
            )
        ]

        task = build_task(task_row, job_rows)
        self.assertEqual(task.id, "task_001")
        self.assertEqual(len(task.jobs), 1)
        self.assertEqual(task.jobs[0].id, "job_001")

    def test_build_task_skip_wrong_task_id(self):
        task_row = (
            "task_001", "test",
            TaskStatus.CREATED.value,
            TaskMode.DAG.value,
            None, None, None, "", "2025-01-01 00:00:00"
        )

        job_rows = [
            ("job_999", JobType.CN_DAILY_SYNC.value, "CREATED", "task_other", "{}", "[]", 0, 3, None, None, "", "")
        ]
        task = build_task(task_row, job_rows)
        self.assertEqual(len(task.jobs), 0)


class TestJobConditions(unittest.TestCase):

    @patch("core.task_manager.db")
    def test_job_dag_ok(self, mock_db):
        job = Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1", depends_on=[])
        mock_db.execute.return_value = (0,)
        self.assertTrue(job_dag_ok(job))

    @patch("core.task_manager.db")
    def test_job_singleton_ok(self, mock_db):
        job = Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1")
        mock_db.execute.return_value = (0,)
        self.assertTrue(job_singleton_ok(job))

    def test_job_retry_ok(self):
        job = Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1", retry_count=2, retries=3)
        self.assertTrue(job_retry_ok(job))
        job.retry_count = 3
        self.assertFalse(job_retry_ok(job))

    @patch("core.task_manager.job_dag_ok", return_value=True)
    @patch("core.task_manager.job_concurrency_ok", return_value=True)
    @patch("core.task_manager.job_retry_ok", return_value=True)
    def test_job_can_run(self, a, b, c):
        job = Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1")
        self.assertTrue(job_can_run(job))

    @patch("core.task_manager.job_singleton_ok", return_value=True)
    def test_job_can_create(self, m):
        job = Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1")
        self.assertTrue(job_can_create(job))


class TestTaskConditions(unittest.TestCase):

    @patch("core.task_manager.job_can_create", return_value=True)
    def test_task_is_allowed(self, m):
        task = Task(id="t1", status=TaskStatus.CREATED, mode=TaskMode.DAG)
        task.jobs = [Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1")]
        self.assertTrue(task_is_allowed(task))

    @patch("core.task_manager.job_can_run", return_value=True)
    def test_task_can_run(self, m):
        task = Task(id="t1", status=TaskStatus.CREATED, mode=TaskMode.DAG)
        task.jobs = [Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1")]
        self.assertTrue(task_can_run(task))


class TestTaskManagerListTasks(unittest.TestCase):

    @patch("core.task_manager.db")
    def test_list_tasks(self, mock_db):
        task_rows = [
            ("t1", "test1", TaskStatus.CREATED.value, TaskMode.DAG.value, None, None, None, "", "2025-01-01 00:00:00"),
            ("t2", "test2", TaskStatus.SUCCESS.value, TaskMode.DAG.value, None, None, None, "", "2025-01-01 00:00:00"),
        ]

        # ✅ 字段顺序完全正确！task_id 正确匹配！
        job_rows = [
            ("j1", JobType.CN_DAILY_SYNC.value, "CREATED", "t1", "{}", "[]", 0, 3, None, None, "", ""),
            ("j2", JobType.HK_DAILY_SYNC.value, "SUCCESS", "t2", "{}", "[]", 0, 3, None, None, "", ""),
        ]

        def mock_exe(sql, callback=None, **kw):
            if callback:
                con = MagicMock()
                def exe(q, pa=None):
                    r = MagicMock()
                    if "tasks" in q:
                        r.fetchall.return_value = task_rows
                    elif "jobs" in q:
                        r.fetchall.return_value = job_rows
                    return r
                con.execute = exe
                return callback(con)
            return []

        mock_db.execute = mock_exe
        tasks = task_manager.list_tasks(2)

        self.assertEqual(len(tasks), 2)
        self.assertEqual(len(tasks[0].jobs), 1)
        self.assertEqual(len(tasks[1].jobs), 1)
        self.assertEqual(tasks[0].jobs[0].type, JobType.CN_DAILY_SYNC)
        self.assertEqual(tasks[1].jobs[0].type, JobType.HK_DAILY_SYNC)


class TestTaskManagerCRUD(unittest.TestCase):

    @patch("core.task_manager.db")
    def test_load_task(self, mock_db):
        task_row = ("t1", "test", TaskStatus.CREATED.value, TaskMode.DAG.value, None, None, None, "", "2025-01-01 00:00:00")
        job_rows = [("j1", JobType.CN_DAILY_SYNC.value, "CREATED", "t1", "{}", "[]", 0,3,None,None,"","")]

        def mock_exe(sql, callback=None, **kw):
            if callback:
                con = MagicMock()
                con.execute.side_effect = [
                    MagicMock(fetchone=lambda: task_row),
                    MagicMock(fetchall=lambda: job_rows),
                ]
                return callback(con)
            return None

        mock_db.execute = mock_exe
        task = task_manager.load_task("t1")
        self.assertEqual(task.id, "t1")
        self.assertEqual(len(task.jobs), 1)

    @patch("core.task_manager.db")
    def test_save_task(self, mock_db):
        task = Task(id="t1", description="test", status=TaskStatus.CREATED, mode=TaskMode.DAG, create_time="2025-01-01 00:00:00")
        task.jobs = []
        task_manager.save_task(task)
        self.assertTrue(mock_db.execute.called)

    @patch("core.task_manager.db")
    def test_update_job_status(self, mock_db):
        job = Job(id="j1", type=JobType.CN_DAILY_SYNC, status=JobStatus.CREATED, task_id="t1")
        task_manager.update_job_status(job, JobStatus.RUNNING)
        self.assertEqual(job.status, JobStatus.RUNNING)

    @patch("core.task_manager.db")
    def test_update_task_status(self, mock_db):
        task = Task(id="t1", status=TaskStatus.CREATED, mode=TaskMode.DAG)
        task_manager.update_task_status(task, TaskStatus.RUNNING)
        self.assertEqual(task.status, TaskStatus.RUNNING)


if __name__ == '__main__':
    unittest.main()