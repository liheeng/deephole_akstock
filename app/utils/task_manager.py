# app/utils/task_manager.py

import enum
import time
import threading

class TaskStatus(enum.Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
class TaskManager:

    def __init__(self):
        self.lock = threading.Lock()

        self.status = TaskStatus.IDLE
        self.running = False

        self.source = None   # 👈 新增：谁触发的（cron / api）

        self.last_run = None
        self.last_finish = None
        self.message = ""

    def start(self, source="unknown", task_id=None):
        with self.lock:
            if self.running:
                return False

            self.running = True
            self.status = TaskStatus.RUNNING
            self.source = source   # 👈 记录来源
            self.task_id = task_id  # 👈 记录 task_id（可选 ）
            self.last_run = time.strftime("%Y-%m-%d %H:%M:%S")
            self.message = ""

            return True

    def success(self):
        with self.lock:
            self.running = False
            self.status = TaskStatus.SUCCESS
            self.last_finish = time.strftime("%Y-%m-%d %H:%M:%S")

    def fail(self, msg):
        with self.lock:
            self.running = False
            self.status = TaskStatus.FAILED
            self.message = str(msg)
            self.last_finish = time.strftime("%Y-%m-%d %H:%M:%S")

    def get_status(self):
        return {
            "status": self.status,
            "running": self.running,
            "source": self.source,  # 👈 关键
            "task_id": self.task_id,  # 👈 关键
            "last_run": self.last_run,
            "last_finish": self.last_finish,
            "message": self.message
        }


task_manager = TaskManager()