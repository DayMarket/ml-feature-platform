import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/account_id_shop_id/account_shop_features/v1"
ACTION_WINDOWS = (3, 7, 14, 28)
ORDER_WINDOWS = (3, 7, 14, 28, 60, 90)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("account_shop_features_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "account_shop_features_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module(
    "account_shop_features_partition",
    ENTITY / "job/partition.py",
)


class AccountShopFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_account_shop_features_query(
            cls.settings,
            cls.calculated_at,
        )

    def test_feature_families_and_windows(self):
        expected = {
            *(
                f"n_{signal}_{window}d"
                for signal in ("clicks", "atcs", "atfs")
                for window in ACTION_WINDOWS
            ),
            *(f"n_orders_{window}d" for window in ORDER_WINDOWS),
            *(f"gmv_{window}d" for window in ORDER_WINDOWS),
        }
        expected |= {f"{column}_ratio" for column in expected}
        self.assertEqual(set(query.FEATURE_COLUMNS), expected)
        self.assertEqual(len(query.FEATURE_COLUMNS), 48)
        self.assertTrue(
            all(not column.startswith("sid_") for column in query.FEATURE_COLUMNS)
        )

        for window in ORDER_WINDOWS:
            self.assertIn(f"INTERVAL {window} DAYS", self.sql)
        self.assertNotRegex(self.sql, r"INTERVAL (?:30|63|91) DAYS")

    def test_migration_matches_generated_feature_contract(self):
        migration = (ENTITY / "migrations/create_table.sql").read_text(
            encoding="utf-8"
        )
        migration_columns = set(
            re.findall(
                r"^\s{4}([a-z][a-z0-9_]*)\s+(?:INT|DOUBLE|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        expected = {
            "calculated_at",
            "account_id",
            "shop_id",
            *query.FEATURE_COLUMNS,
        }
        self.assertEqual(migration_columns, expected)
        self.assertNotIn("BIGINT", migration)
        self.assertIn("TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')", migration)

    def test_namespace_and_table_contract(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn("feature_namespace: ACCOUNT_SHOP", config_text)
        self.assertIn("primary_key: calculated_at,account_id,shop_id", config_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn('start_date: "2026-09-05T07:00:00Z"', config_text)

    def test_query_topology_is_not_assembled_conditionally(self):
        query_text = (ENTITY / "job/query.py").read_text(encoding="utf-8")
        self.assertNotIn("ctes.append", query_text)
        self.assertNotIn("if settings.has_impressions", query_text)
        self.assertNotIn("if settings.has_recency", query_text)

    def test_actions_are_deduplicated_by_product_session_and_event(self):
        self.assertIn(
            "GROUP BY account_id, session_id, product_id, event_type",
            self.sql,
        )
        self.assertIn("MAX(last_received_at) AS last_received_at", self.sql)
        self.assertIn(
            "'PRODUCT_VIEW', 'ADD_TO_CART', 'ADD_TO_FAVORITES'",
            self.sql,
        )
        self.assertNotIn("n_events", self.sql)

    def test_product_shop_mapping_uses_same_day_s1_snapshot(self):
        self.assertIn(
            "WHERE dt = TIMESTAMP '2026-09-09 19:00:00'",
            self.sql,
        )
        self.assertIn("AND shop_id IS NOT NULL", self.sql)
        self.assertIn("ON action.product_id = product.product_id", self.sql)

    def test_orders_use_sku_mapping_and_shop_grain_distinct(self):
        self.assertIn("ON order_item.sku_id = sku.sku_id", self.sql)
        self.assertIn(
            "'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'",
            self.sql,
        )
        self.assertIn("order_item.b2b_order = FALSE", self.sql)
        self.assertIn("COUNT(DISTINCT CASE", self.sql)
        self.assertIn("GROUP BY account_id, shop_id", self.sql)
        self.assertIn("order_item.payment_price AS DOUBLE", self.sql)
        self.assertIn("order_item.item_quantity AS DOUBLE", self.sql)
        self.assertNotIn("BETWEEN 1", self.sql)
        self.assertNotIn("brand_id", self.sql)

    def test_missing_families_are_zero_and_ratios_use_published_shops(self):
        self.assertIn("COALESCE(actions.n_clicks_3d, 0)", self.sql)
        self.assertIn("COALESCE(orders.n_orders_90d, 0)", self.sql)
        self.assertIn("COALESCE(orders.gmv_90d, 0.0D)", self.sql)
        self.assertIn(
            "SUM(n_orders_90d) OVER (PARTITION BY calculated_at, account_id)",
            self.sql,
        )
        self.assertIn("END AS n_orders_90d_ratio", self.sql)

    def test_cutoffs_are_half_open_and_snapshot_is_local_time(self):
        self.assertIn(
            "TIMESTAMP '2026-09-10 12:00:00' AS calculated_at",
            self.sql,
        )
        self.assertIn(
            "last_received_at < TIMESTAMP '2026-09-10 12:00:00'",
            self.sql,
        )
        self.assertIn(
            "order_item.generated_at < TIMESTAMP '2026-09-10 07:00:00'",
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
        self.assertEqual(
            partition.parse_airflow_timestamp("2026-09-10T07:00:00Z"),
            self.calculated_at,
        )
        with self.assertRaisesRegex(ValueError, "bad-timestamp"):
            partition.parse_airflow_timestamp("bad-timestamp")

    def test_merge_replaces_only_current_snapshot(self):
        merge_sql = query.build_account_shop_features_merge_query(
            "iceberg.gold.target",
            self.settings,
            self.calculated_at,
        )
        self.assertIn(
            "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            merge_sql,
        )
        self.assertIn("target.account_id = source.account_id", merge_sql)
        self.assertIn("target.shop_id = source.shop_id", merge_sql)
        self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)

    def test_dag_waits_for_repository_silvers_and_has_no_active_alerts(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        self.assertEqual(dag_text.count('external_task_id="dq"'), 2)
        self.assertIn("product_id.product_metadata", dag_text)
        self.assertIn("account_product_session_action_counts_12h", dag_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)

    def test_dq_covers_domains_and_window_monotonicity(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        for column in (
            "n_clicks_3d_ratio",
            "n_atcs_28d_ratio",
            "n_atfs_14d_ratio",
            "n_orders_90d_ratio",
            "gmv_60d_ratio",
        ):
            self.assertIn(f"column: {column}", config_text)
        self.assertIn("n_clicks_3d <= n_clicks_7d", config_text)
        self.assertIn("n_orders_60d <= n_orders_90d", config_text)
        self.assertIn("gmv_60d <= gmv_90d", config_text)


if __name__ == "__main__":
    unittest.main()
