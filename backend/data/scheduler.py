"""
backend/data/scheduler.py

Schedules price_collector.collect_today() at 15:05 EET every weekday.

Uses the 'schedule' library in a daemon thread so it does not block
Streamlit. The _started Event makes start_scheduler() idempotent —
safe to call on every Streamlit rerun without spawning duplicate threads.

Local time is assumed to be EET (Cairo, UTC+2) since the app runs locally.
Weekend check (weekday >= 5) is done inside the job to keep the schedule
table simple.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime

import schedule

logger = logging.getLogger(__name__)

_COLLECT_TIME = "15:05"
_started = threading.Event()


def _job() -> None:
    """Fire collect_today() only on weekdays (Mon=0 … Thu=3)."""
    if datetime.now().weekday() in (4, 5):
        logger.debug("scheduler: weekend — skipping collection")
        return
    logger.info("scheduler: %s trigger — running collect_today()", _COLLECT_TIME)
    try:
        from backend.data.price_collector import collect_today
        result = collect_today()
        logger.info("scheduler: collect_today result: %s", result)
    except Exception as exc:
        logger.error("scheduler: collect_today raised: %s", exc)


def _loop() -> None:
    while True:
        schedule.run_pending()
        time.sleep(30)


def start_scheduler() -> None:
    """
    Register the 15:05 job and start the background daemon thread.
    Idempotent — safe to call multiple times (Streamlit reruns the module).
    """
    if _started.is_set():
        return

    schedule.every().day.at(_COLLECT_TIME).do(_job)
    t = threading.Thread(target=_loop, daemon=True, name="price-scheduler")
    t.start()
    _started.set()
    logger.info(
        "scheduler: started — collect_today() fires at %s EET on weekdays",
        _COLLECT_TIME,
    )
