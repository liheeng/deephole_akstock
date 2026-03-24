from code_mapping import  DATA_SOURCE_FORMAT
from sources.data_source import DataSource, AKShareAPIType
from markets.exchange import ExchangeType
class MultiSourceAdapter:
    """
    全市场 + 全数据源 自动代码转换器
    支持 ifind / yfinance / akshare(东财/新浪/腾讯) / eastquotation
    """
    def __init__(self, source: DataSource, akshare_api_type: AKShareAPIType|None = None):
        self.source = source
        self.akshare_api_type = akshare_api_type  # eastmoney / sina / tencent

    def convert(self, symbol: str) -> str:
        code = symbol.strip().upper()

        exchange = ExchangeType(code.split(".")[-1])
        code = code.split(".")[0]

        if not exchange:
            raise Exception(f"未知股票代码：{symbol}")

        # ============= A 股 cn_a =============
        if exchange in [ExchangeType.SH, ExchangeType.SZ]:
            if self.source == DataSource.IFIND:
                return f"{code}.{exchange.value}"
            elif self.source == DataSource.YFINANCE:
                suffix = DATA_SOURCE_FORMAT["yfinance"]["cn_a_suffix"][exchange.value]
                return f"{code}.{suffix}"
            elif self.source == DataSource.EASTQUOTATION:
                raise Exception(f"不支持的数据源：{self.source} for {symbol}")
            elif self.source == DataSource.AKSHARE:
                if self.akshare_api_type == AKShareAPIType.EASTMONEY:
                    return code
                elif self.akshare_api_type in [AKShareAPIType.SINA, AKShareAPIType.TENCENT]:
                    return f"{exchange.value.lower()}{code}"

        # ============= 港股 hk =============
        elif exchange == ExchangeType.HK:
            if self.source == DataSource.IFIND:
                return f"{code}.{ExchangeType.HK.value}"
            elif self.source == DataSource.YFINANCE:
                return f"{code}.{ExchangeType.HK.value}"
            elif self.source == DataSource.EASTQUOTATION:
                return code
            elif self.source == DataSource.AKSHARE:
                if self.akshare_api_type in [AKShareAPIType.EASTMONEY, AKShareAPIType.SINA]:
                    return code
                raise Exception(f"不支持的数据源：{self.source} - API: {self.akshare_api_type} for {symbol}")

        # ============= 美股 us =============
        elif exchange in [ExchangeType.NASDAQ, ExchangeType.NYSE, ExchangeType.AMEX]:
            if self.source == DataSource.IFIND:
                suffix = DATA_SOURCE_FORMAT["ifind"]["us_suffix"][exchange.value]
                return f"{code}.{suffix}"
            elif self.source == DataSource.YFINANCE:
                return code
            elif self.source == DataSource.AKSHARE:
                if self.akshare_api_type in [AKShareAPIType.EASTMONEY, AKShareAPIType.SINA]:
                    return code
                raise Exception(f"不支持的数据源：{self.source} - API: {self.akshare_api_type} for {symbol}")
            elif self.source == DataSource.EASTQUOTATION:
                raise Exception(f"不支持的数据源：{self.source} - API: {self.akshare_api_type} for {symbol}")

        raise Exception(f"不支持的市场：{exchange.value} for {symbol}")

    def revert(self, source_code: str) -> str:
        """ 还原成内部统一代码 """
        return source_code.split(".")[-1].upper() if "." in source_code else source_code.upper()
    

if __name__ == "__main__":
    # ====================== 配置 ======================
    # DATA_SOURCE = "akshare"
    # AK_SHARE_API_TYPE = "sina"
    # ===================================================

    adapter = MultiSourceAdapter(DataSource.AKSHARE, AKShareAPIType.SINA)

    symbols = ["600036", "000001", "00700", "AAPL", "MSFT", "JPM"]

    for code in symbols:
        target = adapter.convert(code)
        print(f"[{DataSource.AKSHARE}.{AKShareAPIType.SINA}] {code} → {target}")