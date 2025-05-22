"""adding raw data to database cuz Supabase cannot DII"""

import os
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# getting access from .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# db connection
engine = create_engine(DATABASE_URL)
conn = engine.raw_connection()
cursor = conn.cursor()

# no duplicates allowed
def insert_no_duplicates(df, table_name, conflict_cols):
    if df.empty:
        print(f"[{table_name}] Empty DataFrame – skipping.")
        return

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    insert_stmt = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING;
    """
    try:
        execute_values(cursor, insert_stmt, values)
        conn.commit()
        print(f"[{table_name}] Inserted {len(values)} rows (deduplicated).")
    except Exception as e:
        conn.rollback()
        print(f"[{table_name}] Error during insert: {e}")

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


    insert_no_duplicates(df, "stock_prices", ['symbol', 'date'])
    print(f"Sent: {filename} to db")    

#news loading
df_news = pd.read_csv(news_file)
df_news.columns = [c.lower() for c in df_news.columns] #lower

#sentiment holder
if 'sentiment_score' not in df_news.columns:
    df_news['sentiment_score'] = None
if 'sentiment_label' not in df_news.columns:
    df_news['sentiment_label'] = None

insert_no_duplicates(df_news, "news_entries", ['date', 'content'])
print("News sent to db.")