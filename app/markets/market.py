import enum


class Region(enum.Enum):
    CN = "cn"
    HK = "hk"
    US = "us"


class ExchangeType(enum.Enum):
    SH = "SH"   # Shanghai, China
    SZ = "SZ"   # Shenzhen, China
    HK = "HK"   # Hong Kong
    NASDAQ = "NASDAQ"
    NYSE = "NYSE"
    AMEX = "AMEX"
    OTC = "OTC"


class SymbolType(enum.Enum):
    STOCK = "stock"
    INDEX = "index"
    FUTURE = "future"
    OPTION = "option"
    OTC_PREFERRED = "otc/preferred"