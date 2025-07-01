import os
import sys
from pathlib import Path
from io import StringIO
import psycopg2
from dotenv import load_dotenv

# 1) Make 'scripts' package importable
airflow_dir = Path(__file__).resolve().parents[2]   # …/Weather_ETL_Pipeline/airflow
sys.path.insert(0, str(airflow_dir))

# 2) Load .env from project root
project_root = airflow_dir.parent                      # …/Weather_ETL_Pipeline
load_dotenv(project_root / ".env")

from scripts.transform.transform_api import build_api_df

# 3) Read DB credentials
PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE = (
    os.getenv(v) for v in ("PGUSER","PGPASSWORD","PGHOST","PGPORT","PGDATABASE")
)
MAIN_TBL = "weather_data"
CURR_TBL = "this_hour_weather_data"

def load_weather_data() -> None:
    """
    - Build API DataFrame.
    - Bulk insert into MAIN_TBL.
    - Truncate & reload into CURR_TBL.
    """
    if not all([PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE]):
        raise ValueError("Missing DB env vars")

    df = build_api_df()
    if df.empty:
        return    # nothing to load

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        port=PGPORT
    )
    try:
        buf = StringIO()
        df.to_csv(buf, sep="\t", index=False, header=False)
        buf.seek(0)

        with conn.cursor() as cur:
            # load full history
            cur.copy_from(buf, MAIN_TBL, sep="\t", null="", columns=tuple(df.columns))
            buf.seek(0)
            # refresh current-hour table
            cur.execute(f"TRUNCATE {CURR_TBL}")
            cur.copy_from(buf, CURR_TBL, sep="\t", null="", columns=tuple(df.columns))

        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    load_weather_data()
