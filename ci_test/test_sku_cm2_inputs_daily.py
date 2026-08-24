import importlib.util
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY_PATH = (
    ROOT / "layers" / "silver" / "sku_id" / "sku_cm2_inputs_daily" / "v1"
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class SkuCm2InputsDailyRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_module(
            ENTITY_PATH / "job" / "runtime.py",
            "test_sku_cm2_inputs_daily_runtime",
        )

    def test_timestamp_parser_accepts_airflow_formats(self):
        values = (
            "2026-08-06T19:00:00",
            "2026-08-06T19:00:00+00:00",
            "2026-08-06T19:00:00Z",
            "2026-08-06 19:00:00+00:00",
            "2026-08-07T00:00:00+05:00",
        )

        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.parse_airflow_timestamp(value),
                    datetime(2026, 8, 6, 19, tzinfo=timezone.utc),
                )
                self.assertEqual(
                    self.runtime.previous_tashkent_date(value),
                    date(2026, 8, 6),
                )

    def test_timestamp_parser_rejects_invalid_value(self):
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            self.runtime.parse_airflow_timestamp("not-a-timestamp")

    def test_config_matches_contract(self):
        config = self.runtime.load_config(ENTITY_PATH / "config.yaml")

        self.assertEqual(config["source"]["trino_conn_id"], "trino_recsys")
        self.assertEqual(config["runtime"]["resources"]["cpu"], "1")
        self.assertEqual(config["runtime"]["resources"]["memory"], "4Gi")
        self.assertEqual(
            config["table"]["primary_key"],
            "dt,sku_id",
        )
        self.assertEqual(
            self.runtime.OUTPUT_COLUMNS,
            (
                "dt",
                "sku_id",
                "product_id",
                "dimensional_group",
                "sell_price_uzs",
                "commission_pct",
                "n_orders_28d",
            ),
        )

    def test_table_ref_uses_two_part_pyiceberg_identifier(self):
        ref = self.runtime.table_ref(
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "silver",
                    "name": "feature_platform_sku_cm2_inputs_daily",
                }
            }
        )

        self.assertEqual(
            ref.identifier,
            ("silver", "feature_platform_sku_cm2_inputs_daily"),
        )

    def test_table_ref_rejects_malformed_identifiers(self):
        malformed = (
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "",
                    "name": "table",
                }
            },
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "silver",
                    "name": "silver.table",
                }
            },
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "silver",
                    "name": "iceberg.silver.table",
                }
            },
        )

        for config in malformed:
            with self.subTest(config=config), self.assertRaises(ValueError):
                self.runtime.table_ref(config)

    def test_preflight_uses_exact_two_part_identifier(self):
        ref = self.runtime.TableRef(
            catalog="iceberg",
            schema="silver",
            name="feature_platform_sku_cm2_inputs_daily",
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
                    ("silver", "feature_platform_sku_cm2_inputs_daily"),
                ),
                (
                    "load_table",
                    ("silver", "feature_platform_sku_cm2_inputs_daily"),
                ),
            ],
        )


class SkuCm2InputsDailyMigrationTest(unittest.TestCase):
    def test_output_schema_matches_contract(self):
        migration = (ENTITY_PATH / "migrations" / "create_table.sql").read_text(
            encoding="utf-8"
        )

        for column in (
            "dt DATE",
            "sku_id INT",
            "product_id INT",
            "dimensional_group STRING",
            "sell_price_uzs DOUBLE",
            "commission_pct DOUBLE",
            "n_orders_28d BIGINT",
        ):
            self.assertIn(column, migration)
        for forbidden in (
            "sku_group_id",
            "currency",
            "weighted_price",
            "net_inflow",
        ):
            self.assertNotIn(forbidden, migration)
        self.assertNotIn("snapshot", migration.lower())
        self.assertIn("PARTITIONED BY (dt)", migration)
        self.assertIn("'engine.hive.lock-enabled' = 'false'", migration)


if __name__ == "__main__":
    unittest.main()
