"""
Fundamental analysis engine.
DCF valuation, P/E, P/B, debt ratios for the long-term wallet.
TODO: implement DCF and ratio calculations using yfinance financials.
"""
import pandas as pd


def dcf(free_cash_flows: list[float], growth_rate: float, discount_rate: float, terminal_growth: float) -> float:
    """Simple DCF intrinsic value estimate."""
    raise NotImplementedError


def debt_to_equity(ticker_info: dict) -> float:
    return ticker_info.get("debtToEquity", float("nan"))


def pe_ratio(ticker_info: dict) -> float:
    return ticker_info.get("trailingPE", float("nan"))
