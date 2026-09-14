"""Reconcile the transformed CSV with the loaded DuckDB rows."""

import csv
import logging
import math
from datetime import datetime

import duckdb


logger = logging.getLogger(__name__)


def validate_weather(input_path: str, database_path: str) -> int:
    """Check that every CSV row exists in DuckDB with matching values."""

    with open(input_path, encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    if not rows:
        raise ValueError("Cannot validate an empty weather dataset")

    connection = duckdb.connect(database_path, read_only=True)

    try:
        for row in rows:
            key = (
                row["city"],
                datetime.fromisoformat(row["forecast_time_utc"]),
            )

            actual = connection.execute("""
                SELECT temperature_c, humidity_pct, precipitation_mm
                FROM weather_hourly
                WHERE city = ? AND forecast_time_utc = ?
            """, key).fetchone()

            if actual is None:
                raise ValueError(f"Missing database row: {key}")

            expected = (
                float(row["temperature_c"]),
                float(row["humidity_pct"]),
                float(row["precipitation_mm"]),
            )

            for field, expected_value, actual_value in zip(
                ("temperature_c", "humidity_pct", "precipitation_mm"),
                expected,
                actual,
                strict=True,
            ):
                if actual_value is None or not math.isclose(
                    expected_value,
                    actual_value,
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                ):
                    raise ValueError(
                        f"Mismatch for {key}, field {field}: "
                        f"expected {expected_value}, got {actual_value}"
                    )

    finally:
        connection.close()

    logger.info("Validated %s CSV rows against DuckDB", len(rows))
    return len(rows)