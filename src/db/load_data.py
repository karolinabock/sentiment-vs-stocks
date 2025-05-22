"""adding raw data to database cuz Supabase cannot DII"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# getting access from .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# db connection
engine = create_engine(DATABASE_URL)

# paths
stock_files = ['AAPL.csv', 'TSLA.csv', 'AMZN.csv']
stock_folder = 'data/raw/'
news_file = 'data/raw/mkt_news.csv'

#loading data from yfinance
for filename in stock_files:
    path = os.path.join(stock_folder, filename)
    symbol = filename.split('.')[0]
    df = pd.read_csv(path)

    #add columnf if not exisit
    if 'symbol' not in df.columns:
        df['symbol'] = symbol

    df = df[['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df.columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']

    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # to None

    df['date'] = pd.to_datetime(df['date'], errors='coerce')  # data conv
    df.dropna(subset=['date', 'open', 'close'], inplace=True)  # delete invalid rows


    df.to_sql('stock_prices', engine, if_exists='append', index=False)
    print(f"Sent: {filename} to db")    

#news loading
df_news = pd.read_csv(news_file)
df_news.columns = [c.lower() for c in df_news.columns] #lower

#sentiment holder
if 'sentiment_score' not in df_news.columns:
    df_news['sentiment_score'] = None
if 'sentiment_label' not in df_news.columns:
    df_news['sentiment_label'] = None

df_news.to_sql('news_entries', engine, if_exists='append', index=False)
print("News sent to db.")