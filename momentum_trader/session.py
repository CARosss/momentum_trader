from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from .config import RUN_PROFILES, RunMode, RunProfile, TRADING_TIMEZONE
from .scheduler import CronScheduler, sleep_until
from .trader import MomentumTrader


def _default_sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)


@dataclass
class TradingSession:
    """Coordinates bootstrapping and scheduled execution of the trading strategy."""

    trader: MomentumTrader
    profile: RunProfile
    sleep_fn: Callable[[float], None] = _default_sleep

    def __post_init__(self) -> None:
        self.scheduler = CronScheduler(self.profile.cron_expression, TRADING_TIMEZONE)
        self.retry_delay = timedelta(minutes=5)

    def bootstrap(self, *, wait_for_positions_seconds: int = 60) -> None:
        """Initial setup: clear positions, prime capital, and build the first portfolio."""
        account = self.trader.api.get_account()
        buying_power = float(account.buying_power)
        print(f"Initial account buying power: ${buying_power:,.2f}")
        self.trader.set_initial_buying_power(buying_power)

        print("Selling all existing positions before starting session...")
        self.trader.sell_all_positions()
        self.sleep_fn(float(wait_for_positions_seconds))

        refreshed_account = self.trader.api.get_account()
        refreshed_buying_power = float(refreshed_account.buying_power)
        print(f"Buying power after liquidation: ${refreshed_buying_power:,.2f}")

        tickers = self.trader.load_tickers()
        print(f"Loaded {len(tickers)} tickers from Nasdaq-100.")

        snapshot = self.trader.update_momentum_snapshot()
        print(f"Initial momentum snapshot generated for {snapshot.as_of}.")
        self.trader.rebalance(snapshot.weights)
        print("Initial portfolio established.")

    def run_forever(self) -> None:
        """Run the trading cycle on the configured schedule."""
        print(f"Starting trading loop using profile '{self.profile.mode.value}'.")
        while True:
            try:
                now = datetime.now(TRADING_TIMEZONE)
                next_run = self._next_valid_run(now)
                wait_seconds = (next_run - now).total_seconds()
                print(
                    f"Next execution scheduled for {next_run}. Sleeping for {wait_seconds:.0f} seconds."
                )
                sleep_until(next_run, self.sleep_fn)
                self._execute_cycle()
            except Exception as exc:  # noqa: BLE001 - top-level loop must catch all exceptions
                print(f"Unhandled error during trading cycle: {exc}")
                print(f"Retrying in {self.retry_delay.total_seconds()} seconds.")
                self.sleep_fn(self.retry_delay.total_seconds())

    def _execute_cycle(self) -> None:
        now = datetime.now(TRADING_TIMEZONE)
        print(f"Executing trading cycle at {now}.")
        if self.profile.requires_market_open and not self.trader.is_market_open():
            print("Market is closed; skipping this run.")
            return

        snapshot = self.trader.update_momentum_snapshot()
        print(f"Momentum snapshot updated for {snapshot.as_of}. Rebalancing portfolio.")
        self.trader.rebalance(snapshot.weights)

    def _next_valid_run(self, reference: datetime) -> datetime:
        candidate = self.scheduler.next_run_after(reference)
        while self.profile.requires_trading_day and not self.trader.is_trading_day(candidate):
            candidate = self.scheduler.next_run_after(candidate + timedelta(minutes=1))
        return candidate

    @classmethod
    def from_mode(
        cls,
        mode: RunMode,
        trader: MomentumTrader,
        sleep_fn: Callable[[float], None] = _default_sleep,
    ) -> "TradingSession":
        profile = RUN_PROFILES[mode]
        return cls(trader=trader, profile=profile, sleep_fn=sleep_fn)
