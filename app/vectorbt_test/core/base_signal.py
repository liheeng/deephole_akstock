# signals/base_signal.py
from abc import abstractmethod

import pandas as pd


class BaseSignal:
    @property
    def name(self) -> str:
        return "BaseSignal"

    @abstractmethod
    def generate(self, data: pd.DataFrame) -> pd.Series:
        """
        return: pd.Series
        1 / -1 / 0
        """
        raise NotImplementedError
