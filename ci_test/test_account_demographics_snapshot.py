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
    / "snapshot_date_account_id"
    / "account_demographics_snapshot"
    / "v1"
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class AccountDemographicsQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_module(
            ENTITY_PATH / "job" / "query.py",
            "test_account_demographics_query",
        )
        cls.sql = cls.query.build_query(
            snapshot_date=date(2026, 8, 5),
            customer_table='"dwh-iceberg".silver.customer',
            ecosystem_users_table='"ch-ecosystem".ecosystem.ecosystem_users',
            birth_date_placeholder="1970-01-01",
        )

    def test_query_uses_both_sources_and_full_population(self):
        self.assertIn('"dwh-iceberg".silver.customer', self.sql)
        self.assertIn('"ch-ecosystem".ecosystem.ecosystem_users', self.sql)
        self.assertIn("FULL OUTER JOIN ecosystem", self.sql)
        self.assertIn("COALESCE(um.account_id, ecosystem.account_id)", self.sql)

    def test_query_normalizes_gender_with_um_priority(self):
        self.assertIn("WHEN 'MAN' THEN 'M'", self.sql)
        self.assertIn("WHEN 'WOMAN' THEN 'F'", self.sql)
        self.assertIn("WHEN 'M' THEN 'M'", self.sql)
        self.assertIn("WHEN 'F' THEN 'F'", self.sql)
        self.assertIn("COALESCE(um.gender, ecosystem.gender)", self.sql)

    def test_query_normalizes_birth_date_and_calculates_full_years(self):
        self.assertIn("TRY_CAST(birth_year_UB AS DATE)", self.sql)
        self.assertIn("DATE '1970-01-01'", self.sql)
        self.assertIn("birth_date > DATE '2026-08-05'", self.sql)
        self.assertIn("MONTH(DATE '2026-08-05') < MONTH(birth_date)", self.sql)
        self.assertIn("DAY(DATE '2026-08-05') < DAY(birth_date)", self.sql)
        self.assertNotIn("CURRENT_DATE", self.sql)


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

    def test_snapshot_date_uses_asia_tashkent(self):
        self.assertEqual(
            self.runtime.snapshot_date_from_interval_end(
                "2026-08-04T19:00:00Z",
                "Asia/Tashkent",
            ),
            date(2026, 8, 5),
        )

    def test_timestamp_parser_rejects_invalid_value(self):
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            self.runtime.parse_airflow_timestamp("not-a-timestamp")

    def test_config_matches_contract(self):
        config = self.runtime.load_config(ENTITY_PATH / "config.yaml")

        self.assertEqual(config["source"]["trino_conn_id"], "trino_recsys")
        self.assertEqual(config["snapshot"]["timezone"], "Asia/Tashkent")
        self.assertEqual(
            config["table"]["name"],
            "feature_platform_account_demographics_snapshot",
        )
        self.assertEqual(
            config["table"]["primary_key"],
            "snapshot_date,account_id",
        )

    def test_table_ref_uses_two_part_pyiceberg_identifier(self):
        ref = self.runtime.table_ref(
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "silver",
                    "name": "feature_platform_account_demographics_snapshot",
                }
            }
        )

        self.assertEqual(
            ref.identifier,
            ("silver", "feature_platform_account_demographics_snapshot"),
        )

    def test_table_ref_rejects_combined_identifier(self):
        with self.assertRaises(ValueError):
            self.runtime.table_ref(
                {
                    "table": {
                        "catalog": "iceberg",
                        "schema": "silver",
                        "name": "iceberg.silver.account_demographics_snapshot",
                    }
                }
            )

    def test_preflight_uses_exact_two_part_identifier(self):
        ref = self.runtime.TableRef(
            catalog="iceberg",
            schema="silver",
            name="feature_platform_account_demographics_snapshot",
        )

        class FakeCatalog:
            def __init__(self):
                self.calls = []

            def table_exists(self, identifier):
                self.calls.append(("table_exists", identifier))
                return True

            def load_table(self, identifier):
                self.calls.append(("load_table", identifier))
                return "table"

        catalog = FakeCatalog()

        self.assertEqual(self.runtime.preflight_table(catalog, ref), "table")
        self.assertEqual(
            catalog.calls,
            [
                (
                    "table_exists",
                    ("silver", "feature_platform_account_demographics_snapshot"),
                ),
                (
                    "load_table",
                    ("silver", "feature_platform_account_demographics_snapshot"),
                ),
            ],
        )


class AccountDemographicsMigrationTest(unittest.TestCase):
    def test_output_schema_matches_contract(self):
        migration = (ENTITY_PATH / "migrations" / "create_table.sql").read_text(
            encoding="utf-8"
        )

        for column in (
            "snapshot_date DATE",
            "account_id BIGINT",
            "gender STRING",
            "age INTEGER",
        ):
            self.assertIn(column, migration)
        self.assertNotIn("birth_date", migration)
        self.assertNotIn("birth_year", migration)
        self.assertIn("PARTITIONED BY (snapshot_date)", migration)
        self.assertIn("'engine.hive.lock-enabled' = 'false'", migration)


if __name__ == "__main__":
    unittest.main()
