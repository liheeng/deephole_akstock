# services/stock_download.py
from core.rpc.rpc_service import RPCService
import akshare as ak


class StockDownloadService(RPCService):
    def run(self, params):
        symbol = params["symbol"]
        start = params["start"]
        adjust = params["ajust"]
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start,
            adjust=adjust)

        return {
            "status": "ok",
            "symbol": symbol,
            "rows": len(df),
            "data": df
        }
