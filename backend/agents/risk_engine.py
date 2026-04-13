"""
Risk & allocation engine.
VaR, Kelly criterion, position sizing, hard guardrails.
Enforces min R:R 1:1.2, max combined loss 5% of swing wallet,
sector cap (2 per cycle), T+2 settlement gate.
TODO: implement risk calculations.
"""


class RiskEngine:
    """Portfolio risk calculation and guardrail enforcement."""

    def __init__(self, config: dict):
        self.config = config

    def position_size(self, capital: float, stop_loss_pct: float) -> float:
        raise NotImplementedError

    def check_rr(self, entry: float, target: float, stop: float) -> bool:
        """Returns True if R:R >= min_rr_ratio."""
        raise NotImplementedError

    def var(self, positions: list[dict], confidence: float = 0.95) -> float:
        raise NotImplementedError
