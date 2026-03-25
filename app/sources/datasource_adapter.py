from sources.code_mapping import build_symbol
from markets.market import Region   
from sources.data_source import DataSource, DataSourceAPI
from utils.symbol import fix_preferred_symbol


def convert_symbol(
    symbol: str,
    data_source: DataSource,
    region_type: Region,
    api_type: DataSourceAPI | None = None
):
    code = build_symbol(symbol, data_source, region_type, api_type)
    if region_type == Region.US:
        if data_source == DataSource.YFINANCE:
            code = fix_preferred_symbol(code, "-")
        elif data_source == DataSource.IFIND:
            code = fix_preferred_symbol(code, ".")
    return code

# class MultiDataSourceAdapter:
#     """
#     全市场 + 全数据源 自动代码转换器
#     支持 ifind / yfinance / akshare(东财/新浪/腾讯) / eastquotation
#     """
#     def __init__(self, source: DataSource, source_api_type: DataSourceAPI|None = None):
#         self.source = source
#         self.source_api_type = source_api_type  # eastmoney / sina / tencent

#     def convert_symbol(self, symbol: str) -> str:
#         code = symbol.strip().upper()

#         exchange = ExchangeType(code.split(".")[-1])
#         code = code.split(".")[0]

#         if not exchange:
#             raise Exception(f"未知股票代码：{symbol}")

#         # ============= A 股 cn_a =============
#         if exchange in [ExchangeType.SH, ExchangeType.SZ]:
#             if self.source == DataSource.IFIND:
#                 return f"{code}.{exchange.value}"
#             elif self.source == DataSource.YFINANCE:
#                 suffix = DATA_SOURCE_FORMAT["yfinance"]["cn_a_suffix"][exchange.value]
#                 return f"{code}.{suffix}"
#             elif self.source == DataSource.EASTQUOTATION:
#                 raise Exception(f"不支持的数据源：{self.source} for {symbol}")
#             elif self.source == DataSource.AKSHARE:
#                 if self.source_api_type == DataSourceAPI.EASTMONEY_API:
#                     return code
#                 elif self.source_api_type in [DataSourceAPI.AKSHARE_SINA_API, DataSourceAPI.AKSHARE_TENCENT_API]:
#                     return f"{exchange.value.lower()}{code}"

#         # ============= 港股 hk =============
#         elif exchange == ExchangeType.HK:
#             if self.source == DataSource.IFIND:
#                 return f"{code}.{ExchangeType.HK.value}"
#             elif self.source == DataSource.YFINANCE:
#                 return f"{code}.{ExchangeType.HK.value}"
#             elif self.source == DataSource.EASTQUOTATION:
#                 return code
#             elif self.source == DataSource.AKSHARE:
#                 if self.source_api_type in [DataSourceAPI.EASTMONEY_API, DataSourceAPI.AKSHARE_SINA_API]:
#                     return code
#                 raise Exception(f"不支持的数据源：{self.source} - API: {self.source_api_type} for {symbol}")

#         # ============= 美股 us =============
#         elif exchange in [ExchangeType.NASDAQ, ExchangeType.NYSE, ExchangeType.AMEX]:
#             if self.source == DataSource.IFIND:
#                 suffix = DATA_SOURCE_FORMAT["ifind"]["us_suffix"][exchange.value]
#                 return f"{code}.{suffix}"
#             elif self.source == DataSource.YFINANCE:
#                 return code
#             elif self.source == DataSource.AKSHARE:
#                 if self.source_api_type in [DataSourceAPI.EASTMONEY_API, DataSourceAPI.AKSHARE_SINA_API]:
#                     return code
#                 raise Exception(f"不支持的数据源：{self.source} - API: {self.source_api_type} for {symbol}")
#             elif self.source == DataSource.EASTQUOTATION:
#                 raise Exception(f"不支持的数据源：{self.source} - API: {self.source_api_type} for {symbol}")

#         raise Exception(f"不支持的市场：{exchange.value} for {symbol}")

#     def revert(self, source_code: str) -> str:
#         """ 还原成内部统一代码 """
#         return source_code.split(".")[-1].upper() if "." in source_code else source_code.upper()
    

# if __name__ == "__main__":
#     # ====================== 配置 ======================
#     # DATA_SOURCE = "akshare"
#     # AK_SHARE_API_TYPE = "sina"
#     # ===================================================

#     adapter = MultiDataSourceAdapter(DataSource.AKSHARE, DataSourceAPI.AKSHARE_SINA_API)

#     symbols = ["600036", "000001", "00700", "AAPL", "MSFT", "JPM"]

#     for code in symbols:
#         target = adapter.convert_symbol(code)
#         print(f"[{DataSource.AKSHARE}.{DataSourceAPI.AKSHARE_SINA_API}] {code} → {target}")