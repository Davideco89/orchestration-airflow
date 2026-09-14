"""Callbacks for Airflow task failures."""

import logging


logger = logging.getLogger(__name__)


def log_task_failure(context):
    """Log the failed task and its execution details."""

    task_instance = context["task_instance"]

    logger.error(
        "WEATHER_TASK_FAILURE | dag=%s | task=%s | run=%s | error=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context["run_id"],
        context.get("exception"),
    )