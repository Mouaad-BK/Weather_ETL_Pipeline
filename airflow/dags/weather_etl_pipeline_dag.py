from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from pendulum import timezone

# Add airflow/ to PYTHONPATH so we can import scripts/
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Import the full ETL pipeline (extract → transform → load)
from scripts.load.load_api import load_weather_data

# Minimal and useful default arguments for task behavior
default_args = {
    'owner': 'mouaad_BK',                   
    'retries': 1,                            # Try once more on failure
    'retry_delay': timedelta(minutes=5),     # Wait 5 min before retry
}

# Set timezone to Morocco
local_tz = timezone("Africa/Casablanca")

# Define the DAG
with DAG(
    dag_id="weather_etl_pipeline",                     # Unique ID of your pipeline
    default_args=default_args,
    start_date=datetime(2024, 5, 1, tzinfo=local_tz),  # Start date with timezone
    schedule_interval="@hourly",                       # Run every hour
    catchup=False,                                     # Don't rerun past executions
    tags=["weather", "ETL", "Pipeline"],               # Helps filter in UI
) as dag:

    # Define the task that runs the full ETL
    run_etl = PythonOperator(
        task_id="run_full_weather_etl",       # Task name in the UI
        python_callable=load_weather_data     # Executes the full pipeline
    )
