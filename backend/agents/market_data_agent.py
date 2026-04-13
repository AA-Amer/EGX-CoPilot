"""
Market data agent — thin wrapper that exposes fetcher.py to the
agent layer, handles caching, and injects macro context.
TODO: add in-memory cache with TTL and FRED macro feed.
"""
from backend.data.fetcher import get_batch_quotes, get_news, get_price_history


class MarketDataAgent:
    """Provides price, news, and macro data to other agents."""

    def __init__(self, config: dict):
        self.config = config

    def prices(self, tickers: list[str], period: str = "6mo") -> dict:
        return {t: get_price_history(t, period=period) for t in tickers}

    def quotes(self, tickers: list[str]) -> dict:
        return get_batch_quotes(tickers)

    def news(self, ticker: str, max_items: int = 10) -> list[dict]:
        return get_news(ticker, max_items=max_items)
