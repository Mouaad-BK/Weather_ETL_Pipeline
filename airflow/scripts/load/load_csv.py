import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv, find_dotenv
from io import StringIO
from pathlib import Path

# Add the Airflow folder to PYTHONPATH so we can import our ETL scripts
airflow_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(airflow_dir))

# Load .env from project root (automatically finds the file)
load_dotenv(find_dotenv())

# Import the CSV transformer
from scripts.transform.transform_csv import build_global_csv_df

# PostgreSQL connection parameters from environment
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")

TABLE = "weather_data"

def main():
    # Ensure all DB vars are set
    if not all([PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE]):
        raise ValueError(
            f"Missing PostgreSQL variables: "
            f"PGUSER={PGUSER}, PGPASSWORD={'***' if PGPASSWORD else None}, "
            f"PGHOST={PGHOST}, PGPORT={PGPORT}, PGDATABASE={PGDATABASE}"
        )

    # Build DataFrame from CSVs
    df = build_global_csv_df()
    if df.empty:
        print("No CSV data to load.")
        return

    # Connect to the database
    conn = psycopg2.connect(
        dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD, host=PGHOST, port=PGPORT
    )
    try:
        # Fetch existing keys to avoid duplicates
        with conn.cursor() as cur:
            cur.execute(f'SELECT "Ville", "Date", "Heure" FROM {TABLE}')
            rows = cur.fetchall()
        existing = pd.DataFrame(rows, columns=["Ville", "Date", "Heure"])

        # Identify new rows only
        merged = df.merge(existing, on=["Ville", "Date", "Heure"], how="left", indicator=True)
        new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        if new_rows.empty:
            print("No new rows to insert.")
            return

        # Bulk insert via COPY
        buffer = StringIO()
        new_rows.to_csv(buffer, sep="\t", index=False, header=False)
        buffer.seek(0)
        with conn.cursor() as cur:
            cur.copy_from(buffer, TABLE, sep="\t", null="", columns=new_rows.columns)
        conn.commit()
        print(f"Inserted {len(new_rows)} new rows into {TABLE}.")
    except Exception as e:
        conn.rollback()
        print(f"Error loading CSV data: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
