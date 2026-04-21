import enum
import os
import io
import csv
import time
from typing import Optional, List, Dict, Any
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
    markets: List[str] | None
    symbols: List[str] | None

    sectors: List[str] | None
    universe: str | None

    start: str
    end: str
    sql: str


class DataSetConfig(BaseModel):
    id: str
    name: str
    createdAt: str
    sourceDef: DataSetSourceDef

    schema: List[str] | None
    rowCount: int

    cache: Optional[dict]
    # : {
    #     status: 'ready' | 'running' | 'error'
    #     tableName?: string
    # }


class BacktestRequest(BaseModel):
    dataset_config: DataSetConfig
    portfolio_config: PortfolioConfig
