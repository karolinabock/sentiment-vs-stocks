import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import dotenv_values
from pathlib import Path
from datetime import datetime, timedelta

# data loader
env_path = Path(__file__).resolve().parents[2] / ".env"
env_vars = dotenv_values(env_path)
DATABASE_URL = env_vars.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)

cutoff_range = datetime.now() - timedelta(days=180)

df_news = pd.read_sql("SELECT id, date, content FROM news_entries", engine)
df_news['date'] = pd.to_datetime(df_news['date'])
df_news = df_news[df_news['date'] >= cutoff_range]

df_sessions = pd.read_sql("SELECT DISTINCT date FROM stock_prices ORDER BY date", engine)
session_dates = pd.to_datetime(df_sessions['date']).sort_values().tolist()

# debug
print(df_news[['id', 'date']].head())


# Cutoff logic
cutoff_hour = 22
mapped = []

for _, row in df_news.iterrows():
    dt = row['date']
    original_day = dt.date()

    # > 22:00 +1day
    if dt.hour >= cutoff_hour:
        target_day = original_day + timedelta(days=1)
    else:
        target_day = original_day

    # next session finder
    mapped_date = next((d.date() for d in session_dates if d.date() >= target_day), None)

    if mapped_date is None and session_dates:
        mapped_date = session_dates[-1].date()

    if mapped_date:
        mapped.append({
            'news_id': row['id'],
            'mapped_date': mapped_date
        })

df_mapped = pd.DataFrame(mapped)
print(f"\n Mapped {len(df_mapped)} news.")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS news_mapped (
            news_id INTEGER PRIMARY KEY,
            mapped_date DATE NOT NULL
        );
    """))
    conn.execute(text("DELETE FROM news_mapped;"))
    df_mapped.to_sql("news_mapped", con=conn, if_exists='append', index=False)
    print("Results saved in news_mapped.")
