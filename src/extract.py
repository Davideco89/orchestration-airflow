"""Extract hourly weather forecasts from Open-Meteo."""

import json
import logging
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


logger = logging.getLogger(__name__)


def extract_weather(output_path: str) -> str:
    """Download one day of forecasts for Milan and save the raw JSON."""

    params = {
        "latitude": 45.46,
        "longitude": 9.19,
        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation"
        ),
        "forecast_days": 1,
        "timezone": "UTC",
    }

    url = (
        "https://api.open-meteo.com/v1/forecast?"
        + urlencode(params)
    )

    logger.info("Requesting weather forecasts for Milan")

    with urlopen(url, timeout=30) as response:
        payload = json.load(response)

    if "hourly" not in payload or "hourly_units" not in payload:
        raise ValueError("API response is missing hourly data or units")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    logger.info("Raw weather response saved to %s", path)

    return str(path)