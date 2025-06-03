from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def log_run(source):
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO run_log (run_time, source)
            VALUES (:run_time, :source)
        """), {
            "run_time": datetime.now(),
            "source": source
        })

# .bat
if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "manual"
    log_run(source)
