# app/executors/duckdb_writer.py

import duckdb
from executors.base import register_executor

@register_executor("write_duckdb")
class DuckDBWriter:

    def __init__(self):
        self.conn = duckdb.connect("data.duckdb")

    def execute(self, job):
        data = job.params["data"]

        self.conn.executemany(
            "INSERT INTO daily_data VALUES (?, ?, ?, ?, ?, ?, ?)",
            data
        )
        return data