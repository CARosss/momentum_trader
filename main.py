import pandas as pd
import numpy as np
import yfinance as yf
import alpaca_trade_api as tradeapi
import time
from datetime import datetime, timedelta
import pytz
import os

# Choose the run type: '10min', 'daily', or 'weekly'
run_type = 'daily'

print("Script started. Initializing variables and connecting to Alpaca API...")

# Alpaca API credentials
API_KEY = os.environ.get('ALPACA_API_KEY')
API_SECRET = os.environ.get('ALPACA_API_SECRET')
BASE_URL = 'https://paper-api.alpaca.markets'  # Use this for paper trading

# Initialize Alpaca API
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

print("Alpaca API connection established.")

# Global variables
df = None
mtl = None
ret_12, ret_6, ret_3 = None, None, None
INITIAL_BUYING_POWER = None  # New global variable to store initial buying power #####


def get_rolling(df, n):
    return df.rolling(n).apply(np.prod)


def get_weighted_portfolio(date):
    print(f"Calculating weighted portfolio for date: {date}")
    top_50 = ret_12.loc[date].nlargest(50)
    top_30 = ret_6.loc[date, top_50.index].nlargest(30)
    top_10 = ret_3.loc[date, top_30.index].nlargest(10)

    # Normalize the returns to use as weights
    weights = top_10 / top_10.sum()

    print(f"Weighted portfolio calculated. Number of stocks: {len(weights)}")
    return weights


def update_data_and_portfolio():
    print("Updating data and portfolio...")
    global df, mtl, ret_12, ret_6, ret_3

    end_date = datetime.now()
    start_date = end_date - timedelta(days=396)  # Approximately 13 months
    df = yf.download(tickers, start_date, end_date)['Adj Close']
    df = df.dropna(axis=1)

    mtl = (df.pct_change() + 1)[1:].resample('D').prod()
    ret_12, ret_6, ret_3 = get_rolling(mtl, 252), get_rolling(mtl, 126), get_rolling(mtl, 63)

    # Get the latest portfolio
    latest_date = mtl.index[-1]
    print(f"Calculating new portfolio weights for date: {latest_date}")
    new_portfolio_weights = get_weighted_portfolio(latest_date)

    print("Data and portfolio update completed.")
    return new_portfolio_weights


def rebalance_portfolio(new_weights):
    global INITIAL_BUYING_POWER  #####
    print("Starting portfolio rebalancing...")
    account = api.get_account()
    total_portfolio_value = float(account.portfolio_value)
    if INITIAL_BUYING_POWER is None:  #####
        INITIAL_BUYING_POWER = float(account.buying_power)  #####
        print(f"Initial buying power set to: ${INITIAL_BUYING_POWER:.2f}")  #####
    available_buying_power = INITIAL_BUYING_POWER * 0.45  # Always use 45% of initial buying power #####

    MIN_TRADE_VALUE = 10

    print(f"Total portfolio value: ${total_portfolio_value:.2f}")
    print(f"Available buying power (45% of initial): ${available_buying_power:.2f}")  #####

    # Refresh positions data
    positions = {p.symbol: p for p in api.list_positions()}
    print(f"Current number of positions: {len(positions)}")

    # Get all open orders
    open_orders = {order.symbol: order for order in api.list_orders(status='open')}

    # First, sell overweight positions
    for symbol, position in positions.items():
        if symbol in open_orders:
            print(f"Cancelling open order for {symbol}")
            api.cancel_order(open_orders[symbol].id)

        current_weight = float(position.market_value) / total_portfolio_value
        target_weight = new_weights.get(symbol, 0)

        if current_weight > target_weight:
            current_shares = int(position.qty)
            current_price = float(position.current_price)
            target_shares = int((total_portfolio_value * target_weight) / current_price)
            shares_to_sell = current_shares - target_shares

            if shares_to_sell > 0:
                sell_value = shares_to_sell * current_price
                if sell_value >= MIN_TRADE_VALUE:
                    print(f"Selling {shares_to_sell} shares of {symbol} for ${sell_value:.2f}")
                    api.submit_order(
                        symbol=symbol,
                        qty=shares_to_sell,
                        side='sell',
                        type='market',
                        time_in_force='day'
                    )

    # Then, buy underweight positions
    for symbol, target_weight in new_weights.items():
        if symbol in open_orders:
            print(f"Cancelling open order for {symbol}")
            api.cancel_order(open_orders[symbol].id)

        current_price = float(api.get_latest_trade(symbol).price)
        target_value = total_portfolio_value * target_weight
        current_value = float(positions[symbol].market_value) if symbol in positions else 0
        value_to_buy = target_value - current_value

        if value_to_buy > 0:
            shares_to_buy = int(min(value_to_buy, available_buying_power) / current_price)
            buy_value = shares_to_buy * current_price

            if buy_value >= MIN_TRADE_VALUE and buy_value <= available_buying_power:
                print(f"Buying {shares_to_buy} shares of {symbol} for ${buy_value:.2f}")
                api.submit_order(
                    symbol=symbol,
                    qty=shares_to_buy,
                    side='buy',
                    type='market',
                    time_in_force='day'
                )
                available_buying_power -= buy_value
            else:
                print(
                    f"Skipping buy for {symbol}: Trade value ${buy_value:.2f} is {'below minimum' if buy_value < MIN_TRADE_VALUE else 'exceeds available buying power'}")

    print("Portfolio rebalancing completed.")
    print(f"Remaining available buying power: ${available_buying_power:.2f}")


def is_market_open():
    clock = api.get_clock()
    return clock.is_open


def get_next_market_open():
    clock = api.get_clock()
    return clock.next_open.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/New_York'))


def get_next_run_time():
    now = datetime.now(pytz.timezone('America/New_York'))
    if run_type == '10min':
        if is_market_open():
            next_run = now + timedelta(minutes=10)
            if next_run.time() > datetime.strptime('16:00', '%H:%M').time():  # If next run is after market close
                next_run = get_next_market_open().replace(hour=9, minute=30)  # Set to next market open
        else:
            next_run = get_next_market_open()
    elif run_type == 'daily':
        next_run = now.replace(hour=15, minute=45, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        while not is_trading_day(next_run):
            next_run += timedelta(days=1)
    elif run_type == 'weekly':
        next_run = now.replace(hour=15, minute=45, second=0, microsecond=0)
        days_ahead = 4 - next_run.weekday()  # 4 = Friday
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        next_run += timedelta(days=days_ahead)
        while not is_trading_day(next_run):
            next_run += timedelta(days=1)
    return next_run


def is_trading_day(date):
    try:
        calendar = api.get_calendar(start=date.strftime('%Y-%m-%d'), end=date.strftime('%Y-%m-%d'))
        return len(calendar) > 0
    except:
        return False


def run_trading_strategy():
    print(f"Starting trading strategy loop with {run_type} updates...")
    while True:
        try:
            now = datetime.now(pytz.timezone('America/New_York'))
            next_run = get_next_run_time()

            sleep_seconds = (next_run - now).total_seconds()
            print(f"Sleeping for {sleep_seconds} seconds until {next_run}")
            time.sleep(sleep_seconds)
            print("Awake!")

            if run_type == '10min' and not is_market_open():
                print(f"Market is closed. Waiting until it opens.")
            else:
                print(f"Updating portfolio on {datetime.now(pytz.timezone('America/New_York'))}")
                new_weights = update_data_and_portfolio()
                rebalance_portfolio(new_weights)

        except Exception as e:
            print(f"An error occurred: {e}")
            print("Waiting for 5 minutes before retrying...")
            time.sleep(300)  # Wait for 5 minutes before retrying


def sell_all_positions():
    print("Selling all current positions...")

    # First, cancel all open orders
    open_orders = api.list_orders(status='open')
    for order in open_orders:
        print(f"Cancelling open order for {order.symbol}")
        try:
            api.cancel_order(order.id)
        except Exception as e:
            print(f"Error cancelling order for {order.symbol}: {e}")

    # Wait a moment for cancellations to process
    time.sleep(5)

    # Now, close all positions
    positions = api.list_positions()
    for position in positions:
        print(f"Closing position: Selling all {position.qty} shares of {position.symbol}")
        try:
            api.close_position(position.symbol)
            print(f"Successfully closed position for {position.symbol}")
        except Exception as e:
            print(f"Error closing position for {position.symbol}: {e}")
            print("Attempting to submit a market sell order instead...")
            try:
                api.submit_order(
                    symbol=position.symbol,
                    qty=position.qty,
                    side='sell',
                    type='market',
                    time_in_force='day'
                )
                print(f"Successfully submitted market sell order for {position.symbol}")
            except Exception as e2:
                print(f"Error submitting market sell order for {position.symbol}: {e2}")

    print("All positions closed or sell orders submitted.")


if __name__ == "__main__":
    print("Script main execution started.")
    account = api.get_account()
    INITIAL_BUYING_POWER = float(account.buying_power)  #####
    print(f"Initial account buying power: ${INITIAL_BUYING_POWER:.2f}")  #####

    print("Selling all existing positions...")
    sell_all_positions()

    print("Waiting for 60 seconds to allow positions to update...")
    time.sleep(60)

    account = api.get_account()
    print(f"Account buying power after selling all positions: ${float(account.buying_power):.2f}")

    print("Let's get trading!")

    print("Loading initial data...")
    ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
    tickers = ticker_df['Ticker'].to_list()
    print(f"Number of tickers loaded: {len(tickers)}")

    print("Performing initial portfolio setup...")
    initial_weights = update_data_and_portfolio()
    rebalance_portfolio(initial_weights)

    print(f"Initial setup complete. Starting the trading loop with {run_type} updates...")
    run_trading_strategy()