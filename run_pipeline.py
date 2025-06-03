import subprocess
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from utils.log_run import log_run

if __name__ == "__main__" and "skiplog" not in sys.argv:
    log_run("manual")

scripts = [
    "src/etl/fetch_prices.py",
    "src/etl/fetch_news.py",
    "src/db/load_data.py",
    "src/process/map_news_to_sessions.py"
]

for script in scripts:
    print(f"\n___Running: {script}___\n")
    try:
        subprocess.run([sys.executable, script], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error in {script}: {e}")
