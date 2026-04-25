import enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel


class ApiStrategyConfig(BaseModel):
    id: str
    name: str
    factorIds: List[str]
    signalId: Optional[str] = None
    config: Dict[str, Any]


class ApiFactorConfig(BaseModel):
    id: str
    name: str
    expr: str


class ApiSignalConfig(BaseModel):
    id: str
    name: str
    expr: str


class ApiPortfolioConfig(BaseModel):
    id: str
    name: str
    portfolio_mode: str
    # strategies: List[StrategyConfig]
    strategy_op: Optional[dict] = None
    schedule_signal: Optional[dict] = None
    params: dict
    strategies: Dict[str, ApiStrategyConfig]
    factors: Dict[str, ApiFactorConfig]
    signals: Dict[str, ApiSignalConfig]
    vote_weights: Optional[dict] = None
    strategy_weights: Optional[dict] = None


class ApiDataSetSourceDefType(enum.Enum):
    PRESET = "preset"
    FILTER = "filter"
    SQL = "sql"


class ApiDataSetSourceDef(BaseModel):
    type: str
    markets: Optional[List[str]] = None
    symbols: Optional[List[str]] = None
    sectors: Optional[List[str]] = None
    universe: Optional[str] = None
    start: str
    end: str
    sql: Optional[str] = None  # 👈 加 = None


class ApiDataSetConfig(BaseModel):
    id: str
    name: str
    createdAt: str
    sourceDef: ApiDataSetSourceDef

    schema: Optional[List[str]] = None  # 👈 加 = None
    rowCount: Optional[int] = None      # 👈 加 = None
    cache: Optional[dict] = None        # 👈 加 = None


class ApiBacktestRequest(BaseModel):
    dataset_config: ApiDataSetConfig
    portfolio_config: ApiPortfolioConfig