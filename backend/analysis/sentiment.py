"""
Sentiment analysis engine.
Sources: Google News RSS + yfinance ticker.news (Arabic supported).
TODO: implement Arabic/English sentiment scoring.
"""
from backend.data.fetcher import get_news


def ticker_sentiment(ticker: str, max_items: int = 20) -> dict:
    """
    Fetch recent news and return a sentiment summary.
    Returns: {"score": float (-1 to 1), "label": str, "item_count": int}
    TODO: run through Claude API or a local sentiment model.
    """
    news = get_news(ticker, max_items=max_items)
    return {"score": 0.0, "label": "neutral", "item_count": len(news), "raw": news}
