# app/executors/download.py

from executors.base import register_executor

@register_executor("download_daily")
class DownloadExecutor:

    def execute(self, job):
        market = job.params["market"]
        symbols = job.params.get("symbols", [])

        # data = fetch_data(market, symbols)

        # return data
        return "None"