from __future__ import annotations

import argparse
import os
import sys

from alpaca_trade_api.rest import REST

from momentum_trader import RunMode, TradingSession
from momentum_trader.trader import MomentumTrader


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Momentum trading bot entry point.")
    parser.add_argument(
        "--mode",
        choices=[mode.value for mode in RunMode],
        default=os.environ.get("RUN_TYPE", RunMode.DAILY.value),
        help="Scheduling mode for the trading loop (default: %(default)s).",
    )
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Run the bootstrap sequence and exit without starting the scheduler.",
    )
    return parser.parse_args()


def create_api() -> REST:
    api_key = os.environ.get("ALPACA_API_KEY")
    api_secret = os.environ.get("ALPACA_API_SECRET")
    base_url = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    if not api_key or not api_secret:
        print("Missing Alpaca API credentials. Set ALPACA_API_KEY and ALPACA_API_SECRET.", file=sys.stderr)
        raise SystemExit(1)

    return REST(api_key, api_secret, base_url, api_version="v2")


def main() -> None:
    args = parse_arguments()
    run_mode = RunMode(args.mode)
    api = create_api()

    trader = MomentumTrader(api=api)
    session = TradingSession.from_mode(run_mode, trader)

    session.bootstrap()

    if args.bootstrap_only:
        print("Bootstrap completed. Exiting as requested.")
        return

    session.run_forever()


if __name__ == "__main__":
    main()
