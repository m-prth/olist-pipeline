"""backfill_data.py — Loops simulate_stream.py over the full historical date range.
Call this once to generate all data batches before running the Airflow pipeline.
"""
import sys
import subprocess
from datetime import datetime, timedelta

# Settings
START_DATE = "2016-09-04"
END_DATE = "2018-05-31"

def run():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        print(f"Generating data for {date_str}...")
        
        # Use sys.executable so the venv Python is used, not the system Python
        subprocess.run([sys.executable, "scripts/simulate_stream.py", "--date", date_str])
        
        current += timedelta(days=1)

if __name__ == "__main__":
    run()