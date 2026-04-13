from typing import Dict, Any
from vectorbt_test.engine.data_adapter import DataAdapter
from vectorbt_test.engine.data_provider import DataProvider


class PortfolioContext(Dict[str, Any]):
    def __init__(
        self,
        # data_provider: DataProvider,
        # data_adapter: DataAdapter,
        **kwargs
    ):
        super().__init__(
            # data_provider=data_provider,
            # data_adapter=data_adapter,
            **kwargs
        )

    @property
    def data_provider(self) -> DataProvider | None:
        return self.get("data_provider")

    @data_provider.setter
    def data_provider(self, provider: DataProvider):
        self["data_provider"] = provider

    @property
    def data_adapter(self) -> DataAdapter | None:
        return self.get("data_adapter")

    @data_adapter.setter
    def data_adapter(self, adapter: DataAdapter):
        self["data_adapter"] = adapter
