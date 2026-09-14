"""Orchestrate weather extraction, transformation, and loading."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from extract import extract_weather
from transform import transform_weather
from load import load_weather
from validate import validate_weather

from callbacks import log_task_failure


RUN_DIRECTORY = (
    "/opt/airflow/data/runs/"
    "{{ run_id | replace(':', '_') }}"
)


with DAG(
    dag_id="weather_etl",
    description="Load hourly weather forecasts into DuckDB",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": log_task_failure,
    },
    tags=["learning", "weather"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_weather,
        op_kwargs={
            "output_path": RUN_DIRECTORY + "/weather.json",
        },
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_weather,
        op_kwargs={
            "input_path": extract_task.output,
            "output_path": RUN_DIRECTORY + "/weather.csv",
        },
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_weather,
        op_kwargs={
            "input_path": transform_task.output,
            "database_path": "/opt/airflow/data/weather.duckdb",
        },
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate_weather,
        op_kwargs={
            "input_path": transform_task.output,
            "database_path": "/opt/airflow/data/weather.duckdb",
        },
    )

    extract_task >> transform_task >> load_task >> validate_task