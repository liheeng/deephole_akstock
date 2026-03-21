# app/core/result_store.py

class ResultStore:
    def __init__(self):
        self.store = {}

    def set(self, job_id, data):
        self.store[job_id] = data

    def get(self, job_id):
        return self.store.get(job_id)


result_store = ResultStore()