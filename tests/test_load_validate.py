import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from src.load import load_weather
from src.validate import validate_weather


class TestLoadValidate(unittest.TestCase):

    def setUp(self):
        """Create an isolated directory and sample data for each test."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        folder = Path(self.temp_dir.name)
        self.csv_path = str(folder / "weather.csv")
        self.db_path = str(folder / "weather.duckdb")

        self.rows = [
            ("Milan", "2026-09-10T00:00:00+00:00", 17.8, 90, 13.0),
            ("Milan", "2026-09-10T01:00:00+00:00", 18.3, 80, 1.7),
        ]
        self.write_csv(self.rows)

    def write_csv(self, rows):
        with open(self.csv_path, "w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([
                "city",
                "forecast_time_utc",
                "temperature_c",
                "humidity_pct",
                "precipitation_mm",
            ])
            writer.writerows(rows)

    def read_database(self):
        connection = duckdb.connect(self.db_path, read_only=True)
        try:
            return connection.execute("""
                SELECT city, temperature_c, humidity_pct, precipitation_mm
                FROM weather_hourly
                ORDER BY forecast_time_utc
            """).fetchall()
        finally:
            connection.close()

    def test_load_and_validate(self):
        processed = load_weather(self.csv_path, self.db_path)

        self.assertEqual(processed, 2)
        self.assertEqual(self.read_database(), [
            ("Milan", 17.8, 90.0, 13.0),
            ("Milan", 18.3, 80.0, 1.7),
        ])
        self.assertEqual(
            validate_weather(self.csv_path, self.db_path), 2
        )

    def test_repeated_load_does_not_duplicate_rows(self):
        load_weather(self.csv_path, self.db_path)
        before = self.read_database()

        load_weather(self.csv_path, self.db_path)

        self.assertEqual(self.read_database(), before)

    def test_existing_forecast_is_updated(self):
        load_weather(self.csv_path, self.db_path)

        self.rows[0] = (
            "Milan", "2026-09-10T00:00:00+00:00", 22.5, 75, 0.0
        )
        self.write_csv(self.rows)
        load_weather(self.csv_path, self.db_path)

        self.assertEqual(self.read_database(), [
            ("Milan", 22.5, 75.0, 0.0),
            ("Milan", 18.3, 80.0, 1.7),
        ])

    def test_invalid_batch_is_rolled_back(self):
        load_weather(self.csv_path, self.db_path)
        before = self.read_database()

        self.write_csv([
            ("Milan", "2026-09-10T02:00:00+00:00", 20, 70, 0),
            ("Milan", "2026-09-10T03:00:00+00:00", 21, 150, 0),
        ])

        with self.assertRaises(duckdb.ConstraintException):
            load_weather(self.csv_path, self.db_path)

        self.assertEqual(self.read_database(), before)

    def test_validation_rejects_missing_row(self):
        load_weather(self.csv_path, self.db_path)

        self.write_csv(self.rows + [
            ("Milan", "2026-09-10T02:00:00+00:00", 20, 70, 0),
        ])

        with self.assertRaisesRegex(ValueError, "Missing database row"):
            validate_weather(self.csv_path, self.db_path)

    def test_validation_rejects_different_value(self):
        load_weather(self.csv_path, self.db_path)

        self.rows[0] = (
            "Milan", "2026-09-10T00:00:00+00:00", 99, 90, 13.0
        )
        self.write_csv(self.rows)

        with self.assertRaisesRegex(ValueError, "field temperature_c"):
            validate_weather(self.csv_path, self.db_path)


if __name__ == "__main__":
    unittest.main()