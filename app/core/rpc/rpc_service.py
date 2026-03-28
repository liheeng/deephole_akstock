from abc import ABC, abstractmethod


class RPCService(ABC):
    @abstractmethod
    def run(self, params: dict):
        """
        params: dict
        return: any (必须可pickle)
        """
        pass
