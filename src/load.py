"""Load validated hourly forecasts into DuckDB."""

import csv
import logging
from datetime import datetime
from pathlib import Path

import duckdb


logger = logging.getLogger(__name__)


def load_weather(input_path: str, database_path: str) -> int:
    """Insert or update forecasts and return the processed row count."""

    with Path(input_path).open(encoding="utf-8", newline="") as file:
        rows = [
            (
                row["city"],
                datetime.fromisoformat(row["forecast_time_utc"]),
                float(row["temperature_c"]),
                float(row["humidity_pct"]),
                float(row["precipitation_mm"]),
            )
            for row in csv.DictReader(file)
        ]

    if not rows:
        raise ValueError("Cannot load an empty weather dataset")

    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(path))

    try:
        connection.execute("BEGIN TRANSACTION")

        connection.execute("""
            CREATE TABLE IF NOT EXISTS weather_hourly (
                city VARCHAR NOT NULL,
                forecast_time_utc TIMESTAMPTZ NOT NULL,
                temperature_c DOUBLE NOT NULL,
                humidity_pct DOUBLE NOT NULL
                    CHECK (humidity_pct BETWEEN 0 AND 100),
                precipitation_mm DOUBLE NOT NULL
                    CHECK (precipitation_mm >= 0),
                PRIMARY KEY (city, forecast_time_utc)
            )
        """)

        connection.executemany("""
            INSERT INTO weather_hourly (
                city,
                forecast_time_utc,
                temperature_c,
                humidity_pct,
                precipitation_mm
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (city, forecast_time_utc)
            DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                humidity_pct = EXCLUDED.humidity_pct,
                precipitation_mm = EXCLUDED.precipitation_mm
        """, rows)

        connection.execute("COMMIT")

    except Exception:
        connection.execute("ROLLBACK")
        raise

    finally:
        connection.close()

    logger.info("Loaded %s weather rows into %s", len(rows), path)
    return len(rows)