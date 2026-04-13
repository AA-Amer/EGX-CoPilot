"""
Memory agent — persists user goals, risk profile, and trade history
across sessions. Backed by a local JSON file.
TODO: implement read/write/search.
"""
import json
from pathlib import Path

MEMORY_FILE = Path("memory.json")


class MemoryAgent:
    """Persistent user context: goals, risk profile, trade history."""

    def __init__(self, path: Path = MEMORY_FILE):
        self.path = path
        self._data: dict = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            return json.loads(self.path.read_text())
        return {"profile": {}, "trades": [], "goals": []}

    def save(self) -> None:
        self.path.write_text(json.dumps(self._data, indent=2, ensure_ascii=False))

    def get_profile(self) -> dict:
        return self._data.get("profile", {})

    def record_trade(self, trade: dict) -> None:
        self._data.setdefault("trades", []).append(trade)
        self.save()
