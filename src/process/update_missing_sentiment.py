import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_batch
from dotenv import dotenv_values
from pathlib import Path

sys.path.append("src")
from process.sentiment import get_sentiment

env_path = Path(__file__).resolve().parents[2] / ".env"
env_vars = dotenv_values(env_path)
DATABASE_URL = env_vars.get("DATABASE_URL")

engine = create_engine(DATABASE_URL)
conn = engine.raw_connection()
cursor = conn.cursor()


# fins rows without sentiment
df_missing = pd.read_sql("""
    SELECT id, content
    FROM news_entries
    WHERE sentiment_score IS NULL
""", engine)

if df_missing.empty:
    print("All rows with sentiment.")
    exit()

print(f"To analyze {len(df_missing)} rows.")

# sentiment
df_missing[['sentiment_score', 'sentiment_label']] = (
    df_missing['content'].apply(get_sentiment).apply(pd.Series)
)

# sent to db
values = list(zip(df_missing['sentiment_score'], df_missing['sentiment_label'], df_missing['id']))

update_stmt = """
    UPDATE news_entries
    SET sentiment_score = %s,
        sentiment_label = %s
    WHERE id = %s;
"""

try:
    execute_batch(cursor, update_stmt, values)
    conn.commit()
except Exception as e:
    conn.rollback()
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()
