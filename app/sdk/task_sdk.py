import requests
import json
from core.log_stream import publish_log

_current_job_id = "-999999"


def init(job_id: str):
    global _current_job_id
    _current_job_id = job_id


# -------- LOG --------
class LogAPI:
    def info(self, msg):
        publish_log(_current_job_id, str(msg), "INFO")

    def error(self, msg):
        publish_log(_current_job_id, str(msg), "ERROR")


log = LogAPI()


# -------- NET --------
class NetAPI:
    def get(self, url, **kwargs):
        log.info(f"GET {url}")
        return requests.get(url, **kwargs).text

    def post(self, url, data=None, json_data=None, **kwargs):
        log.info(f"POST {url}")
        return requests.post(url, data=data, json=json_data, **kwargs).text


net = NetAPI()


# -------- STORAGE（抽象，不碰真实路径）--------
class StorageAPI:
    def write(self, path: str, data: str):
        log.info(f"write -> {path}")
        # 这里只做演示，可以映射到 DB / S3 / 本地目录
        with open(f"/tmp/{path.replace('/', '_')}", "w") as f:
            f.write(data)

    def read(self, path: str):
        log.info(f"read -> {path}")
        with open(f"/tmp/{path.replace('/', '_')}", "r") as f:
            return f.read()


storage = StorageAPI()