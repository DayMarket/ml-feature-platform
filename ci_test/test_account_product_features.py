import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/account_id_product_id/account_product_features/v1"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("account_product_features_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "account_product_features_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module(
    "account_product_features_partition",
    ENTITY / "job/partition.py",
)


class AccountProductFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_account_product_features_query(
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
                r"^\s{4}([a-z][a-z0-9_]*)\s+(?:INT|DOUBLE|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        expected = {
            "calculated_at",
            "account_id",
            "product_id",
            *query.FEATURE_COLUMNS,
        }
        self.assertEqual(migration_columns, expected)
        self.assertNotIn("BIGINT", migration)
        self.assertNotIn("BIGINT", self.sql)
        self.assertTrue(all(not column.startswith("pid_") for column in query.FEATURE_COLUMNS))

    def test_feature_namespace_is_not_duplicated_in_physical_columns(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn("feature_namespace: ACCOUNT_PRODUCT", config_text)
        self.assertIn("n_clicks_7d", query.FEATURE_COLUMNS)
        self.assertNotIn("pid_n_clicks_7d", query.FEATURE_COLUMNS)

    def test_business_sql_is_explicit_and_has_no_feature_fragment_builders(self):
        query_text = (ENTITY / "job/query.py").read_text(encoding="utf-8")
        self.assertNotIn("_conditional_action_counts", query_text)
        self.assertNotIn("_conditional_order_features", query_text)
        self.assertNotIn("_coalesced_base_features", query_text)
        self.assertNotIn("_ratio_expressions", query_text)

    def test_actions_are_deduplicated_by_session_across_the_window(self):
        self.assertIn(
            "GROUP BY account_id, product_id, event_type, session_id",
            self.sql,
        )
        self.assertIn("COUNT(DISTINCT CASE", self.sql)
        self.assertNotIn("n_events", self.sql)

    def test_orders_use_sku_mapping_success_status_and_transaction_gmv(self):
        self.assertIn("order_item.sku_id AS INT) = sku.sku_id", self.sql)
        self.assertIn(
            "order_item.order_item_status IN (\n"
            "            'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'\n"
            "        )",
            self.sql,
        )
        self.assertIn("order_item.payment_price AS DOUBLE", self.sql)
        self.assertIn("order_item.item_quantity AS DOUBLE", self.sql)
        self.assertIn("COUNT(DISTINCT CASE", self.sql)
        self.assertIn("order_item.b2b_order = FALSE", self.sql)
        self.assertNotIn("BETWEEN 1", self.sql)
        self.assertNotIn("order_item.order_id > 0", self.sql)

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

    def test_click_and_purchase_recency_are_fractional_days_without_rounding(self):
        self.assertIn("AS neg_n_days_since_last_click", self.sql)
        self.assertIn("AS neg_n_days_since_last_purchase", self.sql)
        self.assertGreaterEqual(self.sql.count("/ 86400.0"), 2)
        self.assertNotIn("neg_n_hours_since_last_click", self.sql)
        self.assertNotIn("CEIL(", self.sql)

        migration = (ENTITY / "migrations/create_table.sql").read_text(
            encoding="utf-8"
        )
        for column in (
            "neg_n_days_since_last_click",
            "neg_n_days_since_last_click_rel",
            "neg_n_days_since_last_purchase",
            "n_days_between_last_click_and_last_purchase",
        ):
            self.assertRegex(migration, rf"(?m)^\s+{column} DOUBLE\b")

    def test_legacy_click_purchase_flag_compares_timestamps_in_right_direction(self):
        self.assertIn(
            "TO_UTC_TIMESTAMP(last_click_at, 'Asia/Tashkent') "
            "> last_purchase_at THEN 1",
            self.sql,
        )

    def test_click_purchase_interval_is_signed_fractional_days_without_coalesce(self):
        self.assertIn(
            "UNIX_TIMESTAMP(last_purchase_at)\n"
            "            - UNIX_TIMESTAMP(TO_UTC_TIMESTAMP("
            "last_click_at, 'Asia/Tashkent'))",
            self.sql,
        )
        self.assertIn(
            "/ 86400.0 AS n_days_between_last_click_and_last_purchase",
            self.sql,
        )
        feature_line = next(
            line
            for line in self.sql.splitlines()
            if "AS n_days_between_last_click_and_last_purchase" in line
        )
        self.assertNotIn("COALESCE", feature_line)

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
        merge_sql = query.build_account_product_features_merge_query(
            "iceberg.gold.target",
            self.settings,
            self.calculated_at,
        )
        self.assertIn(
            "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            merge_sql,
        )
        self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)

    def test_dag_waits_for_silver_owner_dq_and_uses_small_profile(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn('external_task_id="dq"', dag_text)
        self.assertIn("account_product_session_action_counts_12h", dag_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn("severity: P3", config_text)
        self.assertIn("oncall_webhook_conn_id: oncall_webhook_recsys", config_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)

    def test_dq_covers_relative_recency_group_invariant(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn("- name: group_max_equals", config_text)
        self.assertIn("column: neg_n_days_since_last_click_rel", config_text)
        self.assertIn("group_by: [account_id]", config_text)


if __name__ == "__main__":
    unittest.main()
