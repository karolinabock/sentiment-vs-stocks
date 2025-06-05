"""sent yfinance data and news with sentiment analize to db"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import subprocess
from pathlib import Path


# path to sentiment module
sys.path.append("src")
from process.sentiment import get_sentiment

# import url
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

print("Connecting to db.")

# db conncetion
engine = create_engine(DATABASE_URL)
conn = engine.raw_connection()
cursor = conn.cursor()

print("Connected to db.")

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

# sentiment 
#symbol + name form txt
symbol_to_name = {}
stock_symbols = []

with open('data/raw/stock_names.txt', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split(';')
        if len(parts) != 2:
            continue
        sym, name = parts
        sym = sym.strip().upper()
        name = name.strip().lower()
        symbol_to_name[sym] = name
        stock_symbols.append(sym)


# stock 
#stock_files = [f"{symbol}.csv" for symbol in stock_symbols]
stock_folder = 'data/raw/'

for symbol in stock_symbols:
    matched_file = next(
        (f for f in os.listdir(stock_folder) if f.startswith(symbol + ";") and f.endswith(".csv")),
        None
    )
    if not matched_file:
        print(f"File not found for symbol: {symbol}")
        continue

    path = os.path.join(stock_folder, matched_file)

    if not os.path.exists(path):
        print(f"File not found: {path}")
        continue

    print(f"Loading: {path}")
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
    print(f"Sent {symbol} to db.")

# news, sentiment, symbol detect

def extract_symbols(text, symbol_map):
    text_upper = text.upper()
    text_lower = text.lower()
    found = [sym for sym, name in symbol_map.items() if sym in text_upper or name in text_lower]
    return found if found else ['UNKNOWN']


news_file = 'data/raw/mkt_news.csv'
df_news = pd.read_csv(news_file)
df_news.columns = [c.lower() for c in df_news.columns]
df_news['content'] = df_news['content'].astype(str)
df_news['date'] = pd.to_datetime(df_news['date'], errors='coerce')
df_news.dropna(subset=['date', 'content'], inplace=True)

print(f"Read {len(df_news)} news entries.")

df_news['symbols'] = df_news['content'].apply(lambda text: extract_symbols(text, symbol_to_name))
df_news = df_news[df_news['symbols'].map(len) > 0]  # discard news with no matched symbol
df_expanded = df_news.explode('symbols').rename(columns={'symbols': 'symbol'})

df_expanded[['sentiment_score', 'sentiment_label']] = df_expanded['content'].apply(get_sentiment).apply(pd.Series)
df_expanded.drop_duplicates(subset=['date', 'content', 'symbol'], inplace=True)

insert_or_update(
    df_expanded[['date', 'content', 'sentiment_score', 'sentiment_label', 'symbol']],
    table_name="news_entries",
    conflict_cols=['date', 'content', 'symbol'],
    update_cols=['sentiment_score', 'sentiment_label']
)

print("News entries (per symbol) saved to db.")

count_result = pd.read_sql("SELECT COUNT(*) AS total FROM news_entries", engine)
print("Total news entries in DB:", count_result['total'][0])