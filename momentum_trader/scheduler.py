from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from croniter import croniter
from pytz import BaseTzInfo


def _ensure_timezone(dt: datetime, tz: BaseTzInfo) -> datetime:
    """Return the datetime localized to the provided timezone."""
    if dt.tzinfo is None:
        return tz.localize(dt)
    return dt.astimezone(tz)


@dataclass
class CronScheduler:
    """Compute successive run times from a cron expression."""

    cron_expression: str
    timezone: BaseTzInfo

    def next_run_after(self, reference: datetime) -> datetime:
        """Return the next run strictly after the provided reference time."""
        localized_reference = _ensure_timezone(reference, self.timezone)
        iterator = croniter(self.cron_expression, localized_reference + timedelta(seconds=1))
        next_candidate = iterator.get_next(datetime)
        return _ensure_timezone(next_candidate, self.timezone)


def sleep_until(target: datetime, sleeper: Callable[[float], None]) -> None:
    """Sleep until the target time using the provided sleeper function."""
    now = datetime.now(target.tzinfo)
    seconds = max(0.0, (target - now).total_seconds())
    sleeper(seconds)

