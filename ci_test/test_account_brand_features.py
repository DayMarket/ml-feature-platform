import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/account_id_brand_id/account_brand_features/v1"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("account_brand_features_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "account_brand_features_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module(
    "account_brand_features_partition",
    ENTITY / "job/partition.py",
)


class AccountBrandFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_account_brand_features_query(
            cls.settings,
            cls.calculated_at,
        )

    def test_fixed_windows_and_statuses(self):
        for window in (3, 7, 14, 28, 60, 90):
            self.assertIn(f"INTERVAL {window} DAYS", self.sql)
        self.assertIn(
            "'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'",
            self.sql,
        )

    def test_migration_matches_all_physical_features(self):
        migration = (ENTITY / "migrations/create_table.sql").read_text(encoding="utf-8")
        migration_columns = set(
            re.findall(
                r"^\s{4}([A-Za-z][A-Za-z0-9_]*)\s+(?:INT|DOUBLE|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        expected = {
            "calculated_at",
            "account_id",
            "brand_id",
            *query.FEATURE_COLUMNS,
        }
        self.assertEqual(migration_columns, expected)
        self.assertNotIn("BIGINT", migration)
        self.assertNotIn("BIGINT", self.sql)
        self.assertNotIn("n_clicks_3d_ratio", migration_columns)
        self.assertTrue(
            all(not column.startswith("bid_") for column in query.FEATURE_COLUMNS)
        )

    def test_feature_namespace_prefixes_every_physical_feature_column(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn("feature_namespace: ACCOUNT_BRAND", config_text)
        self.assertIn("ACCOUNT_BRAND__n_clicks_7d", query.FEATURE_COLUMNS)
        self.assertTrue(
            all(
                column.startswith("ACCOUNT_BRAND__") for column in query.FEATURE_COLUMNS
            )
        )
        self.assertIn("AS ACCOUNT_BRAND__n_clicks_7d", self.sql)
        self.assertNotIn("bid_n_clicks_7d", query.FEATURE_COLUMNS)

    def test_business_sql_uses_only_targeted_feature_expression_builders(self):
        query_text = (ENTITY / "job/query.py").read_text(encoding="utf-8")
        self.assertNotIn("_click_count_expressions", query_text)
        self.assertNotIn("_gmv_expressions", query_text)
        self.assertIn("def _gmv_ratio_expressions()", query_text)

    def test_clicks_are_product_session_counts_mapped_through_daily_s1(self):
        self.assertIn(
            "WHERE dt = TIMESTAMP '2026-09-10 00:00:00'",
            self.sql,
        )
        self.assertIn("event_type = 'PRODUCT_VIEW'", self.sql)
        self.assertIn(
            "GROUP BY\n        account_id,\n        session_id,\n        product_id",
            self.sql,
        )
        self.assertIn("SUM(CASE", self.sql)
        self.assertNotIn("n_events", self.sql)
        self.assertIn("AND account_id IS NOT NULL", self.sql)

    def test_brand_filter_is_applied_only_after_feature_calculation(self):
        click_cte = self.sql.split("click_features AS (", 1)[1].split(
            "),\nsku_mapping AS (", 1
        )[0]
        brand_gmv_cte = self.sql.split("brand_gmv_features AS (", 1)[1].split(
            "),\naccount_gmv_features AS (", 1
        )[0]
        self.assertNotIn("brand_id IS NOT NULL", click_cte)
        self.assertNotIn("brand_id IS NOT NULL", brand_gmv_cte)
        self.assertTrue(self.sql.rstrip().endswith("WHERE brand_id IS NOT NULL"))
        self.assertEqual(self.sql.count("brand_id IS NOT NULL"), 1)
        self.assertNotIn("160078", self.sql)

    def test_orders_use_sku_mapping_statuses_b2b_filter_and_transaction_gmv(self):
        self.assertIn("order_item.sku_id = sku.sku_id", self.sql)
        self.assertNotIn("CAST(order_item.sku_id AS INT)", self.sql)
        self.assertNotIn("CAST(order_item.order_id AS INT)", self.sql)
        self.assertIn(
            "order_item.order_item_status IN (\n"
            "            'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'\n"
            "        )",
            self.sql,
        )
        self.assertIn("order_item.payment_price AS DOUBLE", self.sql)
        self.assertIn("order_item.item_quantity AS DOUBLE", self.sql)
        self.assertIn("order_item.b2b_order = FALSE", self.sql)
        self.assertIn("AND order_item.account_id IS NOT NULL", self.sql)
        self.assertNotIn("CAST(id AS INT) AS sku_id", self.sql)
        self.assertNotIn("CAST(order_item.order_id AS INT)", self.sql)
        self.assertNotIn("CAST(order_item.sku_id AS INT)", self.sql)
        self.assertNotIn("BETWEEN 1", self.sql)

    def test_gmv_denominator_keeps_unbranded_and_unmapped_products(self):
        self.assertIn("LEFT JOIN product_brands product", self.sql)
        account_gmv_cte = self.sql.split("account_gmv_features AS (", 1)[1].split(
            "),\nentity_keys AS (", 1
        )[0]
        self.assertNotIn("brand_id IS NOT NULL", account_gmv_cte)
        self.assertIn("account_gmv.account_gmv_28d > 0", self.sql)
        self.assertIn("/ account_gmv.account_gmv_28d", self.sql)

    def test_cutoffs_are_half_open_and_snapshot_is_local_time(self):
        self.assertIn("TIMESTAMP '2026-09-10 12:00:00' AS calculated_at", self.sql)
        self.assertIn(
            "order_item.generated_at < TIMESTAMP '2026-09-10 07:00:00'",
            self.sql,
        )
        self.assertIn(
            "last_received_at < TIMESTAMP '2026-09-10 12:00:00'",
            self.sql,
        )

    def test_partition_parser_accepts_airflow_timestamps(self):
        self.assertEqual(
            partition.parse_airflow_timestamp("2026-09-10T12:00:00+05:00"),
            self.calculated_at,
        )
        self.assertEqual(
            partition.parse_airflow_timestamp("2026-09-10 07:00:00"),
            self.calculated_at,
        )
        with self.assertRaisesRegex(ValueError, "bad-timestamp"):
            partition.parse_airflow_timestamp("bad-timestamp")

    def test_merge_replaces_only_one_snapshot(self):
        merge_sql = query.build_account_brand_features_merge_query(
            "iceberg.gold.target",
            self.settings,
            self.calculated_at,
        )
        self.assertIn(
            "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            merge_sql,
        )
        self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)

    def test_dag_waits_for_both_silver_dq_tasks_and_has_no_active_alerts(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertEqual(dag_text.count('external_task_id="dq"'), 2)
        self.assertIn("product_id.product_metadata", dag_text)
        self.assertIn("account_product_session_action_counts_12h", dag_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn('start_date: "2026-09-05T07:00:00Z"', config_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)


if __name__ == "__main__":
    unittest.main()
