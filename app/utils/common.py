import time
import random
import os

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
