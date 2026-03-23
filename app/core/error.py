import enum

class ERROR_CODE(enum.Enum):
    ERROR_TASK_NOT_ALLOWED = 1001
    ERROR_TASK_NOT_FOUND = 1002
    ERROR_TASK_CAN_NOT_RUN = 1003

class TaskError(Exception):
    """自定义任务异常类，包含 code 和 message"""
    def __init__(self, code: ERROR_CODE, message: str, error: Exception | None = None):
        # 自定义错误码 + 错误信息
        self.code = code
        self.message = message
        self.error = error
        # 调用父类构造方法
        super().__init__(message)

    def __str__(self):
        # 打印时显示更清晰：[code] message
        return f"[{self.code}] {self.message}"