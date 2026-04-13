"""
API route definitions.
TODO: wire up agents and data endpoints.
"""
from fastapi import APIRouter

router = APIRouter()


@router.get("/quote/{ticker}")
async def quote(ticker: str) -> dict:
    from backend.data.fetcher import get_quote
    return get_quote(ticker)


@router.get("/history/{ticker}")
async def history(ticker: str, period: str = "6mo", interval: str = "1d") -> dict:
    from backend.data.fetcher import get_price_history
    df = get_price_history(ticker, period=period, interval=interval)
    return {"ticker": ticker, "records": df.reset_index().to_dict(orient="records")}


@router.get("/news/{ticker}")
async def news(ticker: str, max_items: int = 10) -> dict:
    from backend.data.fetcher import get_news
    return {"ticker": ticker, "news": get_news(ticker, max_items=max_items)}


@router.post("/chat")
async def chat(body: dict) -> dict:
    # TODO: route to Orchestrator agent
    return {"reply": "Orchestrator not yet implemented."}
