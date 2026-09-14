"""Manually verify retries and the failure callback."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from callbacks import log_task_failure


def fail_intentionally():
    raise RuntimeError("Intentional failure to test retries and callback")


with DAG(
    dag_id="test_failure",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": log_task_failure,
    },
    tags=["learning", "test"],
) as dag:

    failing_task = PythonOperator(
        task_id="fail_intentionally",
        python_callable=fail_intentionally,
    )