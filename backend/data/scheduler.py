"""
Signal scheduler.
Triggers swing signal generation at 17:30 EET and re-checks at 09:45 EET.
TODO: implement with APScheduler or a simple asyncio loop.
"""


def start_scheduler(app) -> None:
    """Attach scheduled jobs to the FastAPI app lifecycle."""
    raise NotImplementedError
