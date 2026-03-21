import enum

class JobType(enum.Enum):
    CN_DAILY_SYNC = "cn_daily_sync" # 同步中国股票的日线数据
    HK_DAILY_SYNC = "hk_daily_sync" # 同步香港股票的日线数据
    US_DAILY_SYNC = "us_daily_sync" # 同步美国股票的日线数据