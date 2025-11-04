from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping

import pytz

# Trading timezone used across the application
TRADING_TIMEZONE = pytz.timezone("America/New_York")


class RunMode(str, Enum):
    """Supported scheduling modes."""

    TEN_MINUTES = "10min"
    DAILY = "daily"
    WEEKLY = "weekly"


@dataclass(frozen=True)
class RunProfile:
    """Configuration for a scheduled trading run."""

    mode: RunMode
    cron_expression: str
    requires_trading_day: bool
    requires_market_open: bool
    description: str


RUN_PROFILES: Mapping[RunMode, RunProfile] = {
    RunMode.TEN_MINUTES: RunProfile(
        mode=RunMode.TEN_MINUTES,
        cron_expression="*/10 9-15 * * MON-FRI",
        requires_trading_day=True,
        requires_market_open=True,
        description="Run every 10 minutes during regular market hours.",
    ),
    RunMode.DAILY: RunProfile(
        mode=RunMode.DAILY,
        cron_expression="45 15 * * MON-FRI",
        requires_trading_day=True,
        requires_market_open=True,
        description="Run once per trading day at 3:45 PM ET.",
    ),
    RunMode.WEEKLY: RunProfile(
        mode=RunMode.WEEKLY,
        cron_expression="45 15 * * FRI",
        requires_trading_day=True,
        requires_market_open=True,
        description="Run once per trading week on Friday at 3:45 PM ET.",
    ),
}

# Always keep trades above this notional value
MIN_TRADE_VALUE_USD = 10.0

# Limit of initial buying power to deploy on each rebalance
BUYING_POWER_FRACTION = 0.45

