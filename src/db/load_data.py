"""sent yfinance data and news with sentiment analize to db"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# path to sentiment module
sys.path.append("src")
from process.sentiment import get_sentiment

# import url
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

print("Connecting to db")

# db conncetion
engine = create_engine(DATABASE_URL)
conn = engine.raw_connection()
cursor = conn.cursor()

# add only when no data
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

def insert_or_update(df, table_name, conflict_cols, update_cols):
    if df.empty:
        print(f"[{table_name}] Empty DataFrame – skipping.")
        return
    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
    insert_stmt = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE
        SET {update_clause};
    """
    try:
        execute_values(cursor, insert_stmt, values)
        conn.commit()
        print(f"[{table_name}] Inserted/Updated {len(values)} rows.")
    except Exception as e:
        conn.rollback()
        print(f"[{table_name}] Error during insert_or_update: {e}")

# paths
stock_files = ['AAPL.csv', 'TSLA.csv', 'AMZN.csv']
stock_folder = 'data/raw/'

for filename in stock_files:
    path = os.path.join(stock_folder, filename)
    symbol = filename.split('.')[0]
    df = pd.read_csv(path)

    if 'symbol' not in df.columns:
        df['symbol'] = symbol

    df = df[['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df.columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']

    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date', 'open', 'close'], inplace=True)

    insert_no_duplicates(df, "stock_prices", ['symbol', 'date'])
    print(f"Sent {filename} to db.")

# sentiment PHASE 1
news_file = 'data/raw/mkt_news.csv'
df_news = pd.read_csv(news_file)
df_news.columns = [c.lower() for c in df_news.columns]

# parsing and sanity check
df_news['content'] = df_news['content'].astype(str)
df_news['date'] = pd.to_datetime(df_news['date'], errors='coerce')
df_news.dropna(subset=['date', 'content'], inplace=True)

print(f"Read {len(df_news)} news entries")

# sentiment
df_news[['sentiment_score', 'sentiment_label']] = df_news['content'].apply(get_sentiment).apply(pd.Series)

df_news.drop_duplicates(subset=['date', 'content'], inplace=True)

# INSERT / UPDATE
insert_or_update(
    df_news,
    table_name="news_entries",
    conflict_cols=['date', 'content'],
    update_cols=['sentiment_score', 'sentiment_label']
)

# DEBUG
print(df_news[['date', 'sentiment_score', 'sentiment_label']].head())
print("News sent to db.")

# final validation
count_result = pd.read_sql("SELECT COUNT(*) AS total FROM news_entries", engine)
print("Total news entries in DB:", count_result['total'][0])
