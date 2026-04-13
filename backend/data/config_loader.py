"""
backend/data/config_loader.py

Loads config.json from the project root and caches it for the session.
Call load_config.cache_clear() after writing new values to config.json
so the next call picks up the changes.
"""
import functools
import json
from pathlib import Path

# Project root is three levels up from this file:
# backend/data/config_loader.py → backend/data → backend → root
ROOT = Path(__file__).parent.parent.parent


@functools.lru_cache(maxsize=1)
def load_config() -> dict:
    """
    Read and return config.json as a dict.
    Result is cached — disk is only read once per process lifetime
    (or after an explicit cache_clear() call).
    """
    config_path = ROOT / "config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
