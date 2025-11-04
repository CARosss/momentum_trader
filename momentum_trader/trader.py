from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

import numpy as np
import pandas as pd
import pytz
import yfinance as yf
from alpaca_trade_api.rest import REST, Calendar, Clock

from .config import BUYING_POWER_FRACTION, MIN_TRADE_VALUE_USD, TRADING_TIMEZONE

@dataclass
class MomentumSnapshot:
    """Represents the most recent momentum weights."""

    as_of: pd.Timestamp
    weights: pd.Series


def _rolling_product(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    """Compute compounded returns over the provided rolling window."""
    return frame.rolling(window).apply(np.prod, raw=True)


class MomentumTrader:
    """High-level orchestration of momentum calculations and portfolio management."""

    def __init__(self, api: REST, *, min_trade_value: float = MIN_TRADE_VALUE_USD) -> None:
        self.api = api
        self.min_trade_value = min_trade_value
        self._tickers: List[str] = []
        self._initial_buying_power: Optional[float] = None

    @property
    def tickers(self) -> List[str]:
        if not self._tickers:
            raise ValueError("Ticker universe not loaded. Call load_tickers() first.")
        return self._tickers

    def load_tickers(self) -> List[str]:
        """Fetch the Nasdaq-100 constituents."""
        table = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
        self._tickers = sorted(set(table["Ticker"].dropna().astype(str)))
        return self._tickers

    def ensure_initial_buying_power(self) -> float:
        """Cache the initial buying power for consistent budget usage."""
        if self._initial_buying_power is None:
            account = self.api.get_account()
            self._initial_buying_power = float(account.buying_power)
        return self._initial_buying_power

    def set_initial_buying_power(self, buying_power: float) -> None:
        """Explicitly set the initial buying power (used during bootstrap)."""
        self._initial_buying_power = buying_power

    def update_momentum_snapshot(self) -> MomentumSnapshot:
        """Download market data and compute the latest weighted portfolio."""
        end_date = datetime.now(pytz.UTC).astimezone(TRADING_TIMEZONE)
        start_date = end_date - timedelta(days=396)
        history = yf.download(self.tickers, start=start_date, end=end_date)["Adj Close"]
        cleaned_history = history.dropna(axis=1)

        if cleaned_history.empty:
            raise ValueError("No historical data retrieved for the configured tickers.")

        momentum_table = (cleaned_history.pct_change() + 1.0).dropna()
        daily_compounded = momentum_table.resample("D").prod()

        if daily_compounded.empty:
            raise ValueError("Insufficient data to compute compounded returns.")

        ret_12 = _rolling_product(daily_compounded, 252)
        ret_6 = _rolling_product(daily_compounded, 126)
        ret_3 = _rolling_product(daily_compounded, 63)

        latest_date = daily_compounded.index[-1]

        latest_weights = self._compute_weights(latest_date, ret_12, ret_6, ret_3)
        return MomentumSnapshot(as_of=latest_date, weights=latest_weights)

    def _compute_weights(
        self,
        date: pd.Timestamp,
        ret_12: pd.DataFrame,
        ret_6: pd.DataFrame,
        ret_3: pd.DataFrame,
    ) -> pd.Series:
        """Apply the nested momentum filter to determine weights."""
        top_50 = ret_12.loc[date].nlargest(50)
        top_30 = ret_6.loc[date, top_50.index].nlargest(30)
        top_10 = ret_3.loc[date, top_30.index].nlargest(10)
        if top_10.empty or float(top_10.sum()) == 0.0:
            raise ValueError("Momentum filter did not produce any candidates.")
        weights = top_10 / top_10.sum()
        return weights

    def rebalance(self, weights: pd.Series) -> None:
        """Rebalance the Alpaca account according to the provided weights."""
        account = self.api.get_account()
        total_portfolio_value = float(account.portfolio_value)

        initial_buying_power = self.ensure_initial_buying_power()
        available_buying_power = initial_buying_power * BUYING_POWER_FRACTION

        positions = {position.symbol: position for position in self.api.list_positions()}
        open_orders = {order.symbol: order for order in self.api.list_orders(status="open")}

        # First reduce overweight positions
        for symbol, position in positions.items():
            if symbol in open_orders:
                self.api.cancel_order(open_orders[symbol].id)

            current_weight = float(position.market_value) / total_portfolio_value
            target_weight = float(weights.get(symbol, 0.0))

            if current_weight > target_weight:
                current_shares = int(float(position.qty))
                current_price = float(position.current_price)
                target_shares = int((total_portfolio_value * target_weight) / current_price)
                shares_to_sell = current_shares - target_shares

                if shares_to_sell <= 0:
                    continue

                sell_value = shares_to_sell * current_price
                if sell_value >= self.min_trade_value:
                    self.api.submit_order(
                        symbol=symbol,
                        qty=shares_to_sell,
                        side="sell",
                        type="market",
                        time_in_force="day",
                    )

        # Then allocate buds to underweight positions
        for symbol, target_weight in weights.items():
            if symbol in open_orders:
                self.api.cancel_order(open_orders[symbol].id)

            latest_trade = self.api.get_latest_trade(symbol)
            current_price = float(latest_trade.price)

            target_value = total_portfolio_value * float(target_weight)
            current_value = float(positions[symbol].market_value) if symbol in positions else 0.0
            budget = min(target_value - current_value, available_buying_power)

            if budget <= 0:
                continue

            shares_to_buy = int(budget / current_price)
            notional_value = shares_to_buy * current_price

            if notional_value >= self.min_trade_value and notional_value <= available_buying_power:
                self.api.submit_order(
                    symbol=symbol,
                    qty=shares_to_buy,
                    side="buy",
                    type="market",
                    time_in_force="day",
                )
                available_buying_power -= notional_value

    def sell_all_positions(self) -> None:
        """Close existing positions and cancel open orders."""
        for order in self.api.list_orders(status="open"):
            try:
                self.api.cancel_order(order.id)
            except Exception:
                # Best-effort cancellation; continue even if an order disappears.
                continue

        for position in self.api.list_positions():
            try:
                self.api.close_position(position.symbol)
            except Exception:
                # Issue a market sell if close_position fails (e.g. for partial fills)
                try:
                    self.api.submit_order(
                        symbol=position.symbol,
                        qty=position.qty,
                        side="sell",
                        type="market",
                        time_in_force="day",
                    )
                except Exception:
                    continue

    def is_market_open(self) -> bool:
        clock: Clock = self.api.get_clock()
        return bool(clock.is_open)

    def next_market_open(self) -> datetime:
        clock: Clock = self.api.get_clock()
        return clock.next_open.replace(tzinfo=pytz.UTC).astimezone(TRADING_TIMEZONE)

    def is_trading_day(self, candidate: datetime) -> bool:
        try:
            calendar: List[Calendar] = self.api.get_calendar(
                start=candidate.strftime("%Y-%m-%d"), end=candidate.strftime("%Y-%m-%d")
            )
            return len(calendar) > 0
        except Exception:
            return False
