import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Generic, TypeVar, Callable, Optional
from utils.log_manager import get_logger

T = TypeVar("T")  # 单个Job返回类型
R = TypeVar("R")  # 最终合并后的结果类型

logger = get_logger(__name__)


# =============================================================================
# ParallelJob：任务自身包含所有回调、参数、名称
# =============================================================================
@dataclass(eq=False)
class ParallelJob(Generic[T]):
    name: str
    parameters: Dict[str, Any]
    job_callback: Callable[["ParallelJob[T]"], T]
    # 支持返回新的 result
    job_result_process_callback: Optional[Callable[[T, "ParallelJob[T]"], T]] = None
    extra_params: Optional[Dict[str, Any]] = None

    # --------------------------
    # 【核心】按对象地址判断相等（唯一标识）
    # --------------------------
    def __eq__(self, other):
        # ✅ 你要的：比较【对象地址】 + 【name 属性】
        if not isinstance(other, ParallelJob):
            return False
        return id(self) == id(other) and self.name == other.name

    # --------------------------
    # 【核心】用对象地址做哈希值（绝对可哈希）
    # --------------------------
    def __hash__(self):
        return hash(id(self))


# =============================================================================
# 并行执行器（结果存储为 DICT：key=job, value=result）
# =============================================================================
class ParallelJobExecutor(Generic[T, R]):
    def __init__(
        self,
        job_result_assemble_callback: Callable[[Dict[ParallelJob[T], T]], R],
        max_workers: int = 5,
        max_retry: int = 3,
        retry_interval: int = 1
    ):
        self.job_result_assemble_callback = job_result_assemble_callback
        self.max_workers = max_workers
        self.max_retry = max_retry
        self.retry_interval = retry_interval

    def _run_job_with_retry(self, job: ParallelJob[T]) -> T | None:
        retries = 0
        while retries < self.max_retry:
            try:
                return job.job_callback(job)
            except Exception as e:
                retries += 1
                logger.warning(f"⚠️ Job [{job.name}] 失败 {retries}/{self.max_retry} | 错误: {str(e)}")
                time.sleep(self.retry_interval)

        logger.error(f"❌ Job [{job.name}] 最终失败！")
        return None

    def execute(self, jobs: List[ParallelJob[T]]) -> R:
        futures = []
        # ======================
        # ✅ 改为字典：key = job, value = result
        # ======================
        success_results: Dict[ParallelJob[T], T] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for job in jobs:
                future = executor.submit(self._run_job_with_retry, job)
                futures.append((job, future))

            for job, future in futures:
                try:
                    result = future.result()
                    if result is not None:
                        # 执行处理回调
                        if job.job_result_process_callback is not None:
                            result = job.job_result_process_callback(result, job)

                        # ✅ 存入字典
                        success_results[job] = result

                except Exception as e:
                    logger.exception(f"❌ Job [{job.name}] 执行异常: ", e)

        # 传入字典给合并函数
        return self.job_result_assemble_callback(success_results)

# ===================================
# Example:
# import pandas as pd

# # 1. 任务执行
# def fetch_job(params):
#     return pd.DataFrame({"code": [params["code"]]})

# # 2. 任务完成处理
# def on_job_done(result, job):
#     result["job_name"] = job.name
#     return result  # 返回新result

# # 3. 合并函数（接收字典）
# def assemble_results(results_dict: Dict[ParallelJob, pd.DataFrame]) -> pd.DataFrame:
#     dfs = list(results_dict.values())
#     return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# # ----------------------
# # 创建任务
# # ----------------------
# job1 = ParallelJob(
#     name="股票000001",
#     parameters={"code": "000001"},
#     job_callback=fetch_job,
#     job_result_process_callback=on_job_done
# )

# # ----------------------
# # 执行
# # ----------------------
# executor = ParallelJobExecutor(
#     job_result_assemble_callback=assemble_results,
#     max_workers=5
# )

# df = executor.execute([job1])