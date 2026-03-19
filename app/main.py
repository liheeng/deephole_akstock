from utils.task_db import generate_task_id, insert_task, finish_task
from utils.http import patch_requests
from utils.task_manager import task_manager
from utils.log_manager import get_task_logger   
from core.updater import Updater
from core.registry import MARKETS


from db.db_common import DB

def main():
    # 👉 启用 UA 注入
    patch_requests()

    updater = Updater(DB)

    for market in MARKETS:
        updater.run(market)

if __name__ == "__main__":
    source = "standalone"

    # init task
    task_id = generate_task_id()
    logger = get_task_logger(task_id)

    logger.info(f"Task started | source={source}")

    if not task_manager.start(source, task_id):
        print(f"[{source}] already running by {task_manager.source}")
        logger.warning(f"[{source}] already running by {task_manager.source}")
        exit(0)

    id = insert_task(task_id, "stock_fetch", source)
    try:
        main()
        
        task_manager.success()
        finish_task(id, "SUCCESS")

        print(f"[{source}] fetch success")
        logger.info(f"[{source}] fetch success")

    except Exception as e:
        task_manager.fail(str(e))
        finish_task(id, "FAILED", str(e))

        print(f"[{source}] fetch failed: {e}")
        logger.error(f"[{source}] fetch failed: {e}")
