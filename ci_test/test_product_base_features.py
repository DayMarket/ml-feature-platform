import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/product_id/product_base_features/v1"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("product_base_features_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "product_base_features_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module(
    "product_base_features_partition",
    ENTITY / "job/partition.py",
)


class ProductBaseFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_product_base_features_query(
            cls.settings,
            cls.calculated_at,
        )

    def test_migration_matches_all_namespaced_features(self):
        migration = (ENTITY / "migrations/create_table.sql").read_text(encoding="utf-8")
        migration_columns = set(
            re.findall(
                r"^\s{4}([A-Za-z][A-Za-z0-9_]*)\s+(?:INT|DOUBLE|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        expected = {"calculated_at", "product_id", *query.FEATURE_COLUMNS}
        self.assertEqual(migration_columns, expected)
        self.assertEqual(len(query.FEATURE_COLUMNS), 102)
        self.assertNotIn("BIGINT", migration)
        self.assertTrue(
            all(column.startswith("PRODUCT__") for column in query.FEATURE_COLUMNS)
        )
        self.assertIn("AS PRODUCT__orders_28d", self.sql)

    def test_population_is_latest_s1_snapshot_and_joins_are_left(self):
        self.assertIn("SELECT MAX(dt) AS dt", self.sql)
        self.assertIn("WHERE dt <= TIMESTAMP '2026-09-10 07:00:00'", self.sql)
        self.assertIn("FROM product_population population", self.sql)
        for relation in (
            "product_prices prices",
            "product_weighted_prices weighted",
            "product_action_features actions",
            "order_and_return_features order_returns",
            "rolling_feedback_features rolling",
            "all_time_feedback_features all_time",
            "category_demographic_features gender",
        ):
            self.assertIn(f"LEFT JOIN {relation}", self.sql)

    def test_actions_sum_source_event_multiplicity_without_impressions(self):
        self.assertIn("THEN n_events ELSE 0 END", self.sql)
        self.assertIn("event_type IN ('PRODUCT_VIEW', 'ADD_TO_FAVORITES')", self.sql)
        self.assertIn("AS clicks_3d", self.sql)
        self.assertIn("AS clicks_28d", self.sql)
        self.assertIn("AS favorites_last_21d", self.sql)
        self.assertNotIn("impression", self.sql.lower())

    def test_orders_use_status_set_and_category_denominator_is_sum_of_products(self):
        self.assertIn(
            "'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'",
            self.sql,
        )
        self.assertIn("order_item.b2b_order = FALSE", self.sql)
        self.assertIn("COUNT(DISTINCT CASE WHEN order_item_status IN", self.sql)
        self.assertIn(
            "SUM(orders_28d) OVER (PARTITION BY category_id)",
            self.sql,
        )
        self.assertIn("AS orders_share_in_category_28d", self.sql)
        self.assertNotIn("BETWEEN 1", self.sql)

    def test_feedback_uses_rating_buckets_and_keeps_smoothing_for_g8(self):
        for rating in range(1, 6):
            self.assertIn(f"n_feedbacks_{rating}", self.sql)
            self.assertIn(
                f"reviews_mark_{['one', 'two', 'three', 'four', 'five'][rating - 1]}_count",
                self.sql,
            )
        self.assertIn("AS feedback_gte_4_ratio_28d", self.sql)
        self.assertIn("AS feedback_lte_3_to_orders_rate_raw", self.sql)
        self.assertNotIn("smoothed", self.sql)
        self.assertNotIn("bad_feedback", self.sql)

    def test_returns_use_status_rows_and_negative_rate(self):
        self.assertIn(
            "THEN 1 ELSE 0 END) AS INT) AS n_completed_3d",
            self.sql,
        )
        self.assertIn(
            "order_item_status = 'RETURNED' THEN 1 ELSE 0 END",
            self.sql,
        )
        self.assertIn("AS return_rate_neg_28d", self.sql)
        self.assertIn("order_item_status = 'RETURNED'", self.sql)
        self.assertIn(
            "'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY', 'RETURNED'",
            self.sql,
        )

    def test_weighted_price_reuses_cm2_threshold_and_sku_order_weights(self):
        self.assertIn(
            "FROM iceberg.silver.feature_platform_sku_cm2_inputs_daily",
            self.sql,
        )
        self.assertIn("WHEN SUM(inputs.n_orders_28d) < 5", self.sql)
        self.assertIn("THEN AVG(inputs.sell_price_uzs)", self.sql)
        self.assertIn(
            "inputs.sell_price_uzs * CAST(inputs.n_orders_28d AS DOUBLE)",
            self.sql,
        )
        self.assertIn("inputs.sell_price_uzs IS NOT NULL", self.sql)
        self.assertIn("inputs.commission_pct IS NOT NULL", self.sql)
        self.assertIn("AS weighted_price", self.sql)

    def test_discount_keeps_missing_sell_price_null(self):
        self.assertIn("WHEN min_sell_price_eod IS NULL", self.sql)
        self.assertIn("THEN NULL", self.sql)

    def test_g6_features_are_mapped_to_each_product(self):
        self.assertIn(
            "CATEGORY_DEMOGRAPHICS__female_product_session_share_28d",
            self.sql,
        )
        self.assertIn(
            "CATEGORY_DEMOGRAPHICS__clicker_age_p10_28d",
            self.sql,
        )
        self.assertIn(
            "AS clicker_age_p50_category_28d",
            self.sql,
        )
        self.assertIn(
            "AS female_unique_clicker_share_category_28d",
            self.sql,
        )
        self.assertIn("AS candidate_female_click_share_28d", self.sql)
        self.assertIn("CAST(category_gender = 'F' AS INT)", self.sql)

    def test_cutoffs_are_half_open_and_output_uses_local_clock(self):
        self.assertIn("TIMESTAMP '2026-09-10 12:00:00' AS calculated_at", self.sql)
        self.assertIn(
            "calculated_at > TIMESTAMP '2026-09-10 12:00:00' - INTERVAL 28 DAYS",
            self.sql,
        )
        self.assertIn("calculated_at <= TIMESTAMP '2026-09-10 12:00:00'", self.sql)
        self.assertIn(
            "order_item.generated_at < TIMESTAMP '2026-09-10 07:00:00'",
            self.sql,
        )

    def test_merge_replaces_only_requested_snapshot(self):
        merge_sql = query.build_product_base_features_merge_query(
            "iceberg.gold.target",
            self.settings,
            self.calculated_at,
        )
        self.assertIn(
            "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            merge_sql,
        )
        self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)

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

    def test_dag_waits_for_all_repository_sources_and_disables_alerts(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertEqual(dag_text.count('external_task_id="dq"'), 7)
        self.assertIn("product_feedback_counts_12h", dag_text)
        self.assertIn("feedback_product_id", dag_text)
        self.assertIn("category_demographic_features", dag_text)
        self.assertIn("sku_cm2_inputs_daily", dag_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn('start_date: "2026-09-05T07:00:00Z"', config_text)
        self.assertIn('"recsys"', dag_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)
        self.assertIn("build_feature_stats_task", dag_text)


if __name__ == "__main__":
    unittest.main()
