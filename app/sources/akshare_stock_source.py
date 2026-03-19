import enum

class StockSource(enum.Enum):
    SSE = 1 # 上交所
    SZSE = 2 # 深交所
    SINA = 3 # 新浪
    EAST = 4 # 东方财富
    XUEQIU = 5 # 雪球
    HKEX = 6 # 港交所
    NYSE = 7 # 纽交所
    NASDAQ = 8 # 纳斯达克
    YAHOO_FINANCE = 9 # 雅虎财经
    TENCENT = 10 # 腾讯财经
    EAST_QUOTATION = 11 # 东方量化
    YFINANCE = 12 # yfinance