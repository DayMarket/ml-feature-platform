import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY_PATH = (
    ROOT / "layers" / "silver" / "product_id" / "product_prices_daily" / "v1"
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ProductPricesDailyQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_module(
            ENTITY_PATH / "job" / "query.py",
            "test_product_prices_daily_query",
        )

    def test_query_uses_one_sku_source_and_two_stage_aggregation(self):
        sql = self.query.build_query(date(2026, 8, 4))

        self.assertIn('"dwh-clickhouse".marts.daily_sku_quantity_eod', sql)
        self.assertIn('"dwh-clickhouse".dict.sku', sql)
        self.assertNotIn('"dwh-iceberg".silver.sku', sql)
        self.assertIn("sku_group_prices AS", sql)
        self.assertIn("MIN(min_sell_price_eod)", sql)
        self.assertIn("AVG(avg_sell_price_eod)", sql)
        self.assertIn("MAX(max_sell_price_eod)", sql)
        self.assertIn("MIN(min_full_price_eod)", sql)
        self.assertIn("MAX(max_full_price_eod)", sql)
        self.assertIn("p.dt = DATE '2026-08-04'", sql)

    def test_query_uses_current_availability(self):
        sql = self.query.build_query(date(2026, 8, 4))

        self.assertIn("s.status = 'ACTIVE'", sql)
        self.assertIn("COALESCE(s.quantity_active, 0) > 0", sql)
        self.assertIn("COALESCE(s.quantity_fbs, 0) > 0", sql)
        self.assertNotIn("quantity_active_eod", sql)

    def test_invalid_prices_are_filtered_independently(self):
        price_sql = self.query.build_query(date(2026, 8, 4))
        metrics_sql = self.query.build_source_metrics_query(date(2026, 8, 4))

        self.assertIn(
            "p.full_price_eod BETWEEN 0 AND 1000000000",
            price_sql,
        )
        self.assertIn(
            "p.sell_price_eod BETWEEN 0 AND 1000000000",
            price_sql,
        )
        self.assertNotIn("negative_price_rows", metrics_sql)
        self.assertNotIn("large_price_rows", metrics_sql)
        self.assertIn("mapping_mismatch_rows", metrics_sql)

    def test_ids_use_integer_and_bang_equal(self):
        price_sql = self.query.build_query(date(2026, 8, 4))
        metrics_sql = self.query.build_source_metrics_query(date(2026, 8, 4))

        self.assertIn("CAST(s.product_id AS INTEGER)", price_sql)
        self.assertIn("CAST(sku_id AS INTEGER)", metrics_sql)
        self.assertIn("s.product_id != p.source_product_id", metrics_sql)


class ProductPricesDailyRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_module(
            ENTITY_PATH / "job" / "runtime.py",
            "test_product_prices_daily_runtime",
        )

    def test_previous_tashkent_date_accepts_airflow_timestamp_formats(self):
        values = (
            "2026-06-17T00:00:00",
            "2026-06-17T00:00:00+00:00",
            "2026-06-17T00:00:00Z",
            "2026-06-17 00:00:00+00:00",
            "2026-06-17 00:00:00",
        )

        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.previous_tashkent_date(value),
                    date(2026, 6, 16),
                )

    def test_previous_tashkent_date_uses_local_midnight_boundary(self):
        values = (
            "2026-06-17T19:00:00Z",
            "2026-06-18T00:00:00+05:00",
        )

        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.previous_tashkent_date(value),
                    date(2026, 6, 17),
                )

    def test_previous_tashkent_date_rejects_unsupported_value(self):
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            self.runtime.previous_tashkent_date("not-a-timestamp")

    def test_table_ref_builds_two_part_pyiceberg_identifier(self):
        ref = self.runtime.table_ref(
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "silver",
                    "name": "feature_platform_product_prices_daily",
                }
            }
        )

        self.assertEqual(
            ref.identifier,
            ("silver", "feature_platform_product_prices_daily"),
        )

    def test_table_ref_rejects_combined_identifiers(self):
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
            name="feature_platform_product_prices_daily",
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
                ("table_exists", ("silver", "feature_platform_product_prices_daily")),
                ("load_table", ("silver", "feature_platform_product_prices_daily")),
            ],
        )

    def test_contract_uses_product_group_and_price_schema(self):
        config = (ENTITY_PATH / "config.yaml").read_text(encoding="utf-8")
        migration = (
            ENTITY_PATH / "migrations" / "create_table.sql"
        ).read_text(encoding="utf-8")
        dag = (ENTITY_PATH / "dag.py").read_text(encoding="utf-8")

        self.assertIn(
            "feature-platform.layers.silver.product_id.product_prices_daily",
            config,
        )
        self.assertIn("product_id INT", migration)
        self.assertIn("min_sell_price_eod DOUBLE", migration)
        self.assertIn("avg_sell_price_eod DOUBLE", migration)
        self.assertIn("max_sell_price_eod DOUBLE", migration)
        self.assertIn("min_full_price_eod DOUBLE", migration)
        self.assertIn("max_full_price_eod DOUBLE", migration)
        self.assertIn(
            "dbt.tests.dbt_clickhouse_dwh.daily_sku_quantity_eod.dq",
            dag,
        )


if __name__ == "__main__":
    unittest.main()
