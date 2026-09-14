"""Introductory DAG with sequential Bash and Python tasks."""

from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def say_hello():
    print("Hello Davide! The Python task ran successfully.")


with DAG(
    dag_id="hello_airflow",
    description="First workflow with Bash and Python tasks",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    print_date = BashOperator(
        task_id="print_date",
        bash_command="date -u",
    )

    greet = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    print_date >> greet