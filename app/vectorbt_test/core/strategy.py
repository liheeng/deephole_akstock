from .factors import FactorNode
from typing import List
from abc import ABC, abstractmethod


class Strategy(ABC):
    @property
    def name(self):
        return self.__class__.__name__
    
    @abstractmethod
    def generate(self, data, cache, context: dict | None = None):
        pass
