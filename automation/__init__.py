from __future__ import annotations

"""
Infrastructure helpers for headless browser automation used to drive
PadlHub/VivaCRM booking flows.

Modules exported here are safe to import from application code.
"""

from .browser import HeadlessBrowser
from .tasks import (
    BookingResult,
    BookingTask,
    BookingTaskManager,
    BookingTaskState,
)

__all__ = [
    "HeadlessBrowser",
    "BookingTask",
    "BookingResult",
    "BookingTaskManager",
    "BookingTaskState",
]


