import time
import random
import os
import enum


def is_running_in_docker() -> bool:
    # 方法1：检查 .dockerenv 文件（最准）
    if os.path.exists("/.dockerenv"):
        return True
    # 方法2：检查 cgroup 信息（兼容大部分容器）
    try:
        with open("/proc/1/cgroup", "rt") as f:
            return "docker" in f.read()
    except:
        pass
    return False


def random_sleep():
    time.sleep(random.uniform(0.5, 1.5))


def batch_sleep(i):
    if i % 50 == 0 and i > 0:
        time.sleep(5)


def date_to_str(d):
    if not d:
        return None
    if isinstance(d, str):
        return d
    return d.strftime("%Y%m%d")


def safe_format(s: str, **kwargs):
    """
    安全格式化字符串：
    存在的变量 → 替换
    不存在的变量 → 保留 {xxx} 原样
    永不报错
    """
    class SafeFormatter(dict):
        def __missing__(self, key):
            return f"{{{key}}}"  # 👈 核心：不存在就返回 {key}
    return s.format_map(SafeFormatter(kwargs))


class ResultStatus(enum.Enum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    FAILED = 2
    CANCELLED = 3


def create_file_with_dirs(file_path: str):
    """
    创建文件，如果目录不存在，自动递归创建多层目录
    如果文件已存在，不会覆盖
    """
    # 获取文件所在的目录
    dir_path = os.path.dirname(file_path)
    
    # 目录不存在 → 递归创建多层目录
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)  # exist_ok=True 就算存在也不报错
    
    # 创建文件（如果不存在）
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("")  # 空文件