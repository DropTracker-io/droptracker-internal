import os
from datetime import timedelta

from dotenv import load_dotenv

from api.services.metrics import MetricsTracker
from utils.logger import LoggerClient
from utils.redis import RedisClient
from services import redis_updates
from db import Session, session


# Load environment variables as early as possible
load_dotenv()


# Core singletons shared across blueprints
logger = LoggerClient(token=os.getenv("LOGGER_TOKEN"))
metrics = MetricsTracker()
redis_client = RedisClient()
redis_tracker = redis_updates.RedisLootTracker()


def get_db_session():
    """Return a new SQLAlchemy session from the shared session factory."""
    return Session()


def reset_db_connections():
    """Dispose of the current scoped session to avoid stale connections."""
    try:
        session.remove()
    except Exception:
        # Be lenient here; this is best-effort cleanup
        pass


__all__ = [
    "logger",
    "metrics",
    "redis_client",
    "redis_tracker",
    "get_db_session",
    "reset_db_connections",
]


