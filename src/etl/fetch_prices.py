"""
    Script Purpose:
Download stock market data (OHLCV) from the last 6 months for companies:
AAPL, TSLA, AMZN, using yfinance and save it e.g. to a .csv file.
"""

# src/etl/fetch_prices.py

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os


# Companies list
with open('data/raw/stock_names.txt') as f:
    symbols = [line.strip() for line in f if line.strip()]

# interval date last 6 months
end_date = datetime.today()
start_date = end_date - timedelta(days=180)

# folder raw creation
os.makedirs("data/raw", exist_ok=True)

# download and saving data
for symbol in symbols:
    print(f"Downloading data for: {symbols}")
    df = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    df.reset_index(inplace=True) # getting DataFrame from yf>make it compatible with powerBI in the future by changing it to normal column 
    output_path = f"data/raw/{symbol}.csv"
    df.to_csv(output_path, index=False) # pandas method
    print(f"Zapisano: {output_path}")