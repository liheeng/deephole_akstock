import multiprocessing as mp
import threading
import uuid
import importlib
import traceback
import sys
import os


def worker(request_q, response_q, pre_worke_callback=None):
    # ⭐ 关键：设置项目路径
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)

    if pre_worke_callback:
        pre_worke_callback()

    while True:
        task = request_q.get()
        if task is None:
            break

        task_id = task["task_id"]
        module_name = task["module"]
        class_name = task["class"]
        params = task["params"]

        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)

            service = cls()
            result = service.run(params)

            response_q.put({
                "task_id": task_id,
                "status": "ok",
                "result": result
            })

        except Exception as e:
            response_q.put({
                "task_id": task_id,
                "status": "error",
                "error": str(e),
                "traceback": traceback.format_exc()
            })


class RPCProcessPool:
    def __init__(self, work_number, pre_work_callback=None):
        self.request_q = mp.Queue()
        self.response_q = mp.Queue()

        self.futures = {}
        self.lock = threading.Lock()

        self.workers = [
            mp.Process(target=worker, args=(self.request_q, self.response_q, pre_work_callback))
            for _ in range(work_number)
        ]

        for p in self.workers:
            p.start()

        self.response_thread = threading.Thread(target=self._response_loop, daemon=True)
        self.response_thread.start()

    def _response_loop(self):
        while True:
            res = self.response_q.get()
            task_id = res["task_id"]

            with self.lock:
                future = self.futures.pop(task_id, None)

            if future:
                future["result"] = res
                future["event"].set()

    def submit(self, module, cls, params, timeout=None):
        task_id = str(uuid.uuid4())
        event = threading.Event()

        with self.lock:
            self.futures[task_id] = {
                "event": event,
                "result": None
            }

        self.request_q.put({
            "task_id": task_id,
            "module": module,
            "class": cls,
            "params": params
        })

        event.wait(timeout=timeout)

        with self.lock:
            future = self.futures.pop(task_id, None)

        if future is None:
            return {"status": "timeout"}

        return future["result"]

    def shutdown(self):
        for _ in self.workers:
            self.request_q.put(None)

        for p in self.workers:
            p.join()

