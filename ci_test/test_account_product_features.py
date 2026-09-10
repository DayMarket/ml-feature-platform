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
        self.assertEqual(self.settings.event_windows_days, (3, 7, 14, 28))
        self.assertEqual(
            self.settings.order_windows_days,
            (3, 7, 14, 28, 60, 90),
        )
        self.assertEqual(
            self.settings.successful_order_statuses,
            ("COMPLETED", "PAID", "DELIVERED", "IN_DELIVERY"),
        )

    def test_migration_matches_all_physical_features(self):
        migration = (ENTITY / "migrations/create_table.sql").read_text(encoding="utf-8")
        migration_columns = set(
            re.findall(
                r"^\s{4}([a-z][a-z0-9_]*)\s+(?:INT|BIGINT|DOUBLE|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        expected = {
            "calculated_at",
            "account_id",
            "product_id",
            *query.feature_columns(self.settings),
        }
        self.assertEqual(migration_columns, expected)

    def test_actions_are_deduplicated_by_session_across_the_window(self):
        self.assertIn(
            "GROUP BY\n        account_id,\n        product_id,\n"
            "        event_type,\n        session_id",
            self.sql,
        )
        self.assertIn("COUNT(DISTINCT CASE", self.sql)
        self.assertNotIn("n_events", self.sql)

    def test_orders_use_sku_mapping_success_status_and_transaction_gmv(self):
        self.assertIn("order_item.sku_id AS BIGINT) = sku.sku_id", self.sql)
        self.assertIn(
            "order_item.order_item_status IN "
            "('COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY')",
            self.sql,
        )
        self.assertIn("order_item.payment_price AS DOUBLE", self.sql)
        self.assertIn("order_item.item_quantity AS DOUBLE", self.sql)
        self.assertIn("COUNT(DISTINCT CASE", self.sql)

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

    def test_click_recency_is_fractional_hours_only(self):
        self.assertIn("AS pid_neg_n_hours_since_last_click", self.sql)
        self.assertIn("/ 3600.0", self.sql)
        self.assertNotIn("pid_neg_n_days_since_last_click", self.sql)

    def test_legacy_click_purchase_flag_compares_timestamps_in_right_direction(self):
        self.assertIn(
            "TO_UTC_TIMESTAMP(last_click_at, 'Asia/Tashkent')\n"
            "                    > last_purchase_at THEN 1",
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


if __name__ == "__main__":
    unittest.main()
