import enum

class ExchangeType(enum.Enum):
    SH = "SH" # Shanghai, China
    SZ = "SZ" # Shenzhen, China
    HK = "HK" # Hong Kong
    NASDAQ = "NASDAQ"
    NYSE = "NYSE"
    AMEX = "AMEX"
    OTC = "OTC"