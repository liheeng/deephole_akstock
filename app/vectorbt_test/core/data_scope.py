class DataScope:
    def __init__(
        self,
        symbols: tuple[str, ...] | None,
        timeframe: str,
        start: str | None = None,
        end: str | None = None,
        dataset: str | None = None
    ):
        self.symbols = symbols
        self.timeframe = timeframe
        self.start = start
        self.end = end
        self.dataset = dataset

    def key(self):
        return (
            self.symbols,
            self.timeframe,
            self.start,
            self.end,
            self.dataset
        )
