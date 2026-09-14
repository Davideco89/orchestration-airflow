"""Transform the raw weather response into validated hourly rows."""

import csv
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path


logger = logging.getLogger(__name__)


def transform_weather(input_path: str, output_path: str) -> str:
    """Validate hourly forecasts and save a normalized CSV."""

    with Path(input_path).open(encoding="utf-8") as file:
        payload = json.load(file)

    if payload.get("utc_offset_seconds") != 0:
        raise ValueError("Expected weather timestamps in UTC")

    expected_units = {
        "time": "iso8601",
        "temperature_2m": "°C",
        "relative_humidity_2m": "%",
        "precipitation": "mm",
    }

    actual_units = payload.get("hourly_units", {})

    for field, expected_unit in expected_units.items():
        actual_unit = actual_units.get(field)

        if actual_unit != expected_unit:
            raise ValueError(
                f"Unexpected unit for {field}: "
                f"expected {expected_unit!r}, got {actual_unit!r}"
            )

    hourly = payload["hourly"]
    fields = [
        "time",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
    ]
    arrays = [hourly[field] for field in fields]

    if not all(isinstance(values, list) for values in arrays):
        raise ValueError("Hourly fields must be arrays")

    if not arrays[0]:
        raise ValueError("No hourly data received")

    rows = []
    seen_times = set()

    for time_value, temperature, humidity, precipitation in zip(
        *arrays, strict=True
    ):
        timestamp = datetime.fromisoformat(time_value)

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        timestamp = timestamp.astimezone(timezone.utc)

        if timestamp in seen_times:
            raise ValueError(f"Duplicate timestamp: {time_value}")

        values = (temperature, humidity, precipitation)
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in values
        ):
            raise ValueError(f"Invalid numeric value at {time_value}")

        if not 0 <= humidity <= 100 or precipitation < 0:
            raise ValueError(f"Invalid humidity or precipitation at {time_value}")

        seen_times.add(timestamp)
        rows.append({
            "city": "Milan",
            "forecast_time_utc": timestamp.isoformat(),
            "temperature_c": temperature,
            "humidity_pct": humidity,
            "precipitation_mm": precipitation,
        })

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Saved %s validated weather rows to %s", len(rows), path)
    return str(path)