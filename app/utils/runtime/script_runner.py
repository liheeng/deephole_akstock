# runtime/script_runner.py

import sys
import traceback
from sdk import task_sdk


def run(script: str, job_id: str):
    # 初始化 SDK（绑定 job_id）
    task_sdk.init(job_id)

    # 构造执行上下文（注入 SDK）
    context = {
        "__builtins__": __builtins__,  # 后面可以换成安全版本
        "net": task_sdk.net,
        "storage": task_sdk.storage,
        "log": task_sdk.log,
    }

    try:
        exec(script, context)
    except Exception as e:
        task_sdk.log.error(str(e))
        traceback.print_exc()
        raise


if __name__ == "__main__":
    job_id = sys.argv[1]
    script = sys.stdin.read()

    run(script, job_id)