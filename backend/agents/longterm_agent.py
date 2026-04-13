"""
Long-term advisor agent.
DCF valuation, fundamental screening, sector rotation analysis.
Minimum holding period: 6 months. Annual target: 25%.
TODO: implement DCF and sector rotation logic.
"""


class LongTermAgent:
    """Fundamental analysis and long-term portfolio advisor."""

    def __init__(self, config: dict):
        self.config = config

    async def screen(self, universe: list[str]) -> list[dict]:
        raise NotImplementedError

    async def value(self, ticker: str) -> dict:
        raise NotImplementedError
