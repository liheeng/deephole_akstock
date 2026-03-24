import enum

class SymbolType(enum.Enum):
    STOCK = "stock"
    INDEX = "index"
    FUTURE = "future"
    OPTION = "option"
    OTC_PREFERRED = "otc/preferred"