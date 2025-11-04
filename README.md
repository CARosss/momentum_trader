### Momentum Trading Bot

#### Strategy

Every (week, day, month..?) look at the the best:
50 performances of 12 months and select
30 best in 6 months and select
10 best in 3 months
from NASDAQ.

Re-balance each refresh appropriately.

Requires own Alpaca trading API keys.

#### Running

The refactored entry point (`main.py`) now wraps the trading logic in typed modules and uses cron-style scheduling. Install the runtime dependencies (e.g. `alpaca-trade-api`, `pandas`, `numpy`, `yfinance`, `croniter`, `pytz`) and export your API credentials:

```bash
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
python main.py --mode daily
```

Supported modes (`--mode` or `RUN_TYPE`) are `10min`, `daily`, and `weekly`. The defaults mirror the previous behaviour; the internal scheduler calculates the next run using cron expressions and enforces trading-day checks. Pass `--bootstrap-only` to perform the initial liquidation/rebalance cycle without entering the loop. If you prefer an external cron daemon, configure it to invoke `python main.py --mode …`; the script will execute a single scheduled cycle and then continue according to its internal timer.

#### Performance

Currently trading daily to test methodology.

#### TODO

- Add some form of fees / leakage
- Test optimal windows and numbers
