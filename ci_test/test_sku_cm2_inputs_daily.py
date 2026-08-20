import importlib.util
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY_PATH = (
    ROOT / "layers" / "silver" / "snapshot_date_sku_id" / "sku_cm2_inputs_daily" / "v1"
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class SkuCm2InputsDailyQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_module(
            ENTITY_PATH / "job" / "query.py",
            "test_sku_cm2_inputs_daily_query",
        )
        cls.query_args = {
            "snapshot_date": date(2026, 8, 5),
            "calculated_at": datetime(2026, 8, 6, tzinfo=timezone.utc),
            "sku_table": '"dwh-clickhouse".dict.sku',
            "prices_table": '"dwh-clickhouse".marts.daily_sku_quantity_eod',
            "commission_table": (
                '"dwh-iceberg".silver_apidb_kazanexpress.public_sku_actual_commission'
            ),
            "commission_column": "comission",
            "orders_table": '"dwh-iceberg".silver.order_item_ue_buyer',
            "orders_lookback_days": 28,
            "default_dimensional_group": "SMALL",
        }

    def test_query_uses_confirmed_sources_and_sku_population(self):
        sql = self.query.build_query(**self.query_args)

        self.assertIn('"dwh-clickhouse".dict.sku', sql)
        self.assertIn('"dwh-clickhouse".marts.daily_sku_quantity_eod', sql)
        self.assertIn(
            '"dwh-iceberg".silver_apidb_kazanexpress.public_sku_actual_commission',
            sql,
        )
        self.assertIn('"dwh-iceberg".silver.order_item_ue_buyer', sql)
        self.assertEqual(sql.count("LEFT JOIN"), 3)
        self.assertIn("WHERE id IS NOT NULL", sql)
        self.assertIn("AND product_id IS NOT NULL", sql)

    def test_query_uses_previous_day_price_and_current_sku_attributes(self):
        sql = self.query.build_query(**self.query_args)

        self.assertIn("WHERE dt = DATE '2026-08-05'", sql)
        self.assertIn("CAST(comission AS DOUBLE) AS commission_pct", sql)
        self.assertIn("CAST(dimensional_group AS VARCHAR)", sql)
        self.assertIn("'SMALL'", sql)

    def test_orders_use_row_count_and_half_open_28_day_window(self):
        sql = self.query.build_query(**self.query_args)

        self.assertIn("CAST(COUNT(*) AS BIGINT) AS n_orders_28d", sql)
        self.assertIn(
            "order_created_at >= TIMESTAMP '2026-07-09 00:00:00'",
            sql,
        )
        self.assertIn(
            "order_created_at < TIMESTAMP '2026-08-06 00:00:00'",
            sql,
        )
        self.assertNotIn("COUNT(DISTINCT order_id)", sql)
        self.assertNotIn("SUM(quantity)", sql)
        self.assertIn("COALESCE(orders.n_orders_28d, 0)", sql)

    def test_output_does_not_publish_gold_or_sku_group_columns(self):
        sql = self.query.build_query(**self.query_args)
        select_sql = sql[sql.rfind("SELECT") :]

        for forbidden in (
            "sku_group_id",
            "currency",
            "weighted_price",
            "net_inflow",
            "cm2",
        ):
            self.assertNotIn(forbidden, select_sql.lower())

    def test_query_does_not_add_a_second_source_metrics_scan(self):
        query_source = (ENTITY_PATH / "job" / "query.py").read_text(encoding="utf-8")
        dag_source = (ENTITY_PATH / "dag.py").read_text(encoding="utf-8")

        self.assertNotIn("build_source_metrics_query", query_source)
        self.assertNotIn("order_metrics", query_source)
        self.assertEqual(dag_source.count("runtime.query_trino("), 1)


class SkuCm2InputsDailyRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_module(
            ENTITY_PATH / "job" / "runtime.py",
            "test_sku_cm2_inputs_daily_runtime",
        )

    def test_timestamp_parser_accepts_airflow_formats(self):
        values = (
            "2026-08-06T00:00:00",
            "2026-08-06T00:00:00+00:00",
            "2026-08-06T00:00:00Z",
            "2026-08-06 00:00:00+00:00",
            "2026-08-06 00:00:00",
        )

        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.parse_airflow_timestamp(value),
                    datetime(2026, 8, 6, tzinfo=timezone.utc),
                )
                self.assertEqual(
                    self.runtime.previous_utc_date(value),
                    date(2026, 8, 5),
                )

    def test_timestamp_parser_rejects_invalid_value(self):
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            self.runtime.parse_airflow_timestamp("not-a-timestamp")

    def test_config_matches_contract(self):
        config = self.runtime.load_config(ENTITY_PATH / "config.yaml")

        self.assertEqual(config["source"]["trino_conn_id"], "trino_recsys")
        self.assertEqual(config["source"]["commission_column"], "comission")
        self.assertEqual(config["source"]["orders_lookback_days"], 28)
        self.assertEqual(config["runtime"]["resources"]["cpu"], "4")
        self.assertEqual(config["runtime"]["resources"]["memory"], "24Gi")
        self.assertEqual(config["dag"]["start_date"], "2026-06-01T00:00:00Z")
        self.assertEqual(
            config["table"]["primary_key"],
            "snapshot_date,sku_id",
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
            "snapshot_date DATE",
            "sku_id BIGINT",
            "product_id BIGINT",
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
        self.assertIn("PARTITIONED BY (snapshot_date)", migration)
        self.assertIn("'engine.hive.lock-enabled' = 'false'", migration)


if __name__ == "__main__":
    unittest.main()
