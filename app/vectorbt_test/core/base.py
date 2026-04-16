import enum
from dataclasses import dataclass


class Scope(enum.Enum):
    TS = "ts"
    CS = "cs"


@dataclass
class GeneralExpr:
    name: str
    expr: str