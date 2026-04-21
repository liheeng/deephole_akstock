import enum
from typing import Optional, List
from pydantic import BaseModel


class StrategyConfig(BaseModel):
    name: str
    factors: List[str]
    signal: Optional[str] = None


class PortfolioConfig(BaseModel):
    name: str
    mode: str
    strategies: List[StrategyConfig]
    strategy_op: Optional[str] = "AND"
    schedule_signal: Optional[str] = None
    params: dict


class DataSetSourceDefType(enum.Enum):
    PRESET = "preset"
    FILTER = "filter"
    SQL = "sql"


class DataSetSourceDef(BaseModel):
    type: str
    markets: Optional[List[str]] = None
    symbols: Optional[List[str]] = None
    sectors: Optional[List[str]] = None
    universe: Optional[str] = None
    start: str
    end: str
    sql: Optional[str] = None  # 👈 加 = None


class DataSetConfig(BaseModel):
    id: str
    name: str
    createdAt: str
    sourceDef: DataSetSourceDef

    schema: Optional[List[str]] = None  # 👈 加 = None
    rowCount: Optional[int] = None      # 👈 加 = None
    cache: Optional[dict] = None        # 👈 加 = None


class BacktestRequest(BaseModel):
    dataset_config: DataSetConfig
    portfolio_config: PortfolioConfig