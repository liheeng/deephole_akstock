from abc import ABC, abstractmethod
from typing import Dict, Any, Generic, TypeVar, Optional, Callable
from dataclasses import dataclass
from loguru import logger



T = TypeVar("T")  # 单个WorkerTask的run方法返回类型


class AbstractExectuorTask(ABC, Generic[T]):
    @abstractmethod
    def run(self, params: Dict[str, Any] | None) -> T:
        pass


@dataclass
class ExectuorTaskCfg:
    task_module_file: str | None = None
    task_class_name: str | None = None
    task_params: Dict[str, Any] | None = None
    call_before_task: Optional[Callable[[AbstractExectuorTask, Dict[str, Any] | None], AbstractExectuorTask]] | None = None
    call_after_task: Optional[Callable[[Dict[str, Any], Dict[str, Any] | None], Dict[str, Any]]] | None = None
