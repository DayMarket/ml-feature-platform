import importlib.util
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY_PATH = (
    ROOT
    / "layers"
    / "silver"
    / "account_id"
    / "account_demographics"
    / "v1"
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class AccountDemographicsRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_module(
            ENTITY_PATH / "job" / "runtime.py",
            "test_account_demographics_runtime",
        )

    def test_timestamp_parser_accepts_airflow_formats(self):
        values = (
            "2026-08-04T19:00:00",
            "2026-08-04T19:00:00+00:00",
            "2026-08-04T19:00:00Z",
            "2026-08-04 19:00:00+00:00",
            "2026-08-04 19:00:00",
        )

        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.parse_airflow_timestamp(value),
                    datetime(2026, 8, 4, 19, 0, tzinfo=timezone.utc),
                )

    def test_dt_uses_asia_tashkent(self):
        self.assertEqual(
            self.runtime.dt_from_interval_end("2026-08-04T19:00:00Z"),
            date(2026, 8, 5),
        )

    def test_timestamp_parser_rejects_invalid_value(self):
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            self.runtime.parse_airflow_timestamp("not-a-timestamp")

    def test_config_matches_contract(self):
        config = self.runtime.load_config(ENTITY_PATH / "config.yaml")

        self.assertEqual(config["source"]["trino_conn_id"], "trino_recsys")
        self.assertEqual(
            config["table"]["name"],
            "feature_platform_account_demographics",
        )
        self.assertEqual(config["table"]["primary_key"], "dt,account_id")
        self.assertEqual(
            self.runtime.OUTPUT_COLUMNS,
            ("dt", "account_id", "gender", "age", "city_name", "platform"),
        )

    def test_table_ref_uses_two_part_pyiceberg_identifier(self):
        ref = self.runtime.table_ref(
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "silver",
                    "name": "feature_platform_account_demographics",
                }
            }
        )

        self.assertEqual(
            ref.identifier,
            ("silver", "feature_platform_account_demographics"),
        )

    def test_table_ref_rejects_combined_identifier(self):
        with self.assertRaises(ValueError):
            self.runtime.table_ref(
                {
                    "table": {
                        "catalog": "iceberg",
                        "schema": "silver",
                        "name": "iceberg.silver.account_demographics",
                    }
                }
            )


class AccountDemographicsMigrationTest(unittest.TestCase):
    def test_output_schema_matches_contract(self):
        migration = (ENTITY_PATH / "migrations" / "create_table.sql").read_text(
            encoding="utf-8"
        )

        for column in (
            "dt DATE",
            "account_id INT",
            "gender STRING",
            "age INTEGER",
            "city_name STRING",
            "platform STRING",
        ):
            self.assertIn(column, migration)
        self.assertNotRegex(migration, r"(?m)^\s*birth_date\s")
        self.assertNotIn("birth_year", migration)
        self.assertNotIn("snapshot", migration.lower())
        self.assertIn("PARTITIONED BY (dt)", migration)
        self.assertIn("'engine.hive.lock-enabled' = 'false'", migration)


if __name__ == "__main__":
    unittest.main()
