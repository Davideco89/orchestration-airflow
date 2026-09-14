import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.transform import transform_weather


class TestTransform(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        folder = Path(self.temp_dir.name)
        self.input_path = folder / "weather.json"
        self.output_path = folder / "weather.csv"

        self.payload = {
            "utc_offset_seconds": 0,
            "hourly_units": {
                "time": "iso8601",
                "temperature_2m": "°C",
                "relative_humidity_2m": "%",
                "precipitation": "mm",
            },
            "hourly": {
                "time": [
                    "2026-09-10T00:00",
                    "2026-09-10T01:00",
                ],
                "temperature_2m": [17.8, 18.3],
                "relative_humidity_2m": [90, 80],
                "precipitation": [13.0, 1.7],
            },
        }

    def run_transform(self):
        with self.input_path.open("w", encoding="utf-8") as file:
            json.dump(self.payload, file, ensure_ascii=False)

        return transform_weather(
            str(self.input_path),
            str(self.output_path),
        )

    def assert_rejected(self, message):
        with self.assertRaisesRegex(ValueError, message):
            self.run_transform()

        self.assertFalse(self.output_path.exists())

    def test_valid_input_produces_expected_csv(self):
        result = self.run_transform()

        self.assertEqual(result, str(self.output_path))

        with self.output_path.open(
            encoding="utf-8", newline=""
        ) as file:
            rows = list(csv.DictReader(file))

        self.assertEqual(rows, [
            {
                "city": "Milan",
                "forecast_time_utc": "2026-09-10T00:00:00+00:00",
                "temperature_c": "17.8",
                "humidity_pct": "90",
                "precipitation_mm": "13.0",
            },
            {
                "city": "Milan",
                "forecast_time_utc": "2026-09-10T01:00:00+00:00",
                "temperature_c": "18.3",
                "humidity_pct": "80",
                "precipitation_mm": "1.7",
            },
        ])

    def test_incorrect_unit_is_rejected(self):
        self.payload["hourly_units"]["temperature_2m"] = "Fahrenheit"

        self.assert_rejected("Unexpected unit for temperature_2m")

    def test_non_utc_offset_is_rejected(self):
        self.payload["utc_offset_seconds"] = 3600

        self.assert_rejected("Expected weather timestamps in UTC")

    def test_empty_data_is_rejected(self):
        for field in self.payload["hourly"]:
            self.payload["hourly"][field] = []

        self.assert_rejected("No hourly data received")

    def test_different_array_lengths_are_rejected(self):
        self.payload["hourly"]["precipitation"].pop()

        self.assert_rejected("zip")

    def test_duplicate_timestamp_is_rejected(self):
        times = self.payload["hourly"]["time"]
        times[1] = times[0]

        self.assert_rejected("Duplicate timestamp")

    def test_invalid_numeric_values_are_rejected(self):
        for value in (None, True, "18.3", float("inf")):
            with self.subTest(value=value):
                self.payload["hourly"]["temperature_2m"][1] = value

                self.assert_rejected("Invalid numeric value")

    def test_invalid_humidity_or_precipitation_is_rejected(self):
        cases = [
            (-1, 0),
            (101, 0),
            (80, -0.1),
        ]

        for humidity, precipitation in cases:
            with self.subTest(
                humidity=humidity,
                precipitation=precipitation,
            ):
                self.payload["hourly"]["relative_humidity_2m"][1] = humidity
                self.payload["hourly"]["precipitation"][1] = precipitation

                self.assert_rejected("Invalid humidity or precipitation")


if __name__ == "__main__":
    unittest.main()