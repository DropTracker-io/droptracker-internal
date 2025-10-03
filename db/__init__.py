"""New ORM package for extracted SQLAlchemy models.

This package migrates models from the legacy `db` module to a proper
Python package while preserving import surface for hot-swappability.
"""

from .models.base import Base, engine, Session, session, xenforo_engine, XenforoSession
from .models import *
from .ops import get_xf_option

__all__ = [
    "Base",
    "engine",
    "Session",
    "session",
    "xenforo_engine",
    "XenforoSession",
    "models",
    "get_current_partition",
    "get_xf_option"
]

