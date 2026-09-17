import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/product_id/product_ranking_features/v1"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("product_ranking_features_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "product_ranking_features_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module(
    "product_ranking_features_partition",
    ENTITY / "job/partition.py",
)


class ProductRankingFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_product_ranking_features_query(
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
        self.assertEqual(len(query.FEATURE_COLUMNS), 21)
        self.assertNotIn("BIGINT", migration)
        self.assertTrue(
            all(
                column.startswith("PRODUCT_STATS__")
                for column in query.FEATURE_COLUMNS
            )
        )

    def test_population_is_the_complete_g7_snapshot(self):
        self.assertIn(
            "FROM iceberg.gold.feature_platform_product_base_features base",
            self.sql,
        )
        self.assertIn(
            "WHERE base.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            self.sql,
        )
        self.assertIn("LEFT JOIN product_category_mapping mapping", self.sql)
        self.assertNotIn("candidate", self.sql.lower())
        self.assertNotIn("account_id", self.sql)

    def test_category_mapping_is_point_in_time_and_not_a_population_filter(self):
        self.assertIn("SELECT MAX(dt) AS dt", self.sql)
        self.assertIn("WHERE dt <= TIMESTAMP '2026-09-10 07:00:00'", self.sql)
        self.assertIn("base.product_id = mapping.product_id", self.sql)
        self.assertIn("category_id IS NOT NULL", self.sql)
        self.assertNotIn("WHERE category_id IS NOT NULL", self.sql)

    def test_g7_columns_are_read_through_the_physical_namespace(self):
        for column in (
            "min_sell_price_eod",
            "orders_28d",
            "clicks_3d",
            "clicks_28d",
            "rating",
            "discount",
            "feedback_quantity",
            "feedback_lte_3",
            "n_completed_28d",
            "n_returned_28d",
            "return_rate_neg_28d",
        ):
            self.assertIn(f"PRODUCT__{column}", self.sql)

    def test_average_rank_and_non_null_percentile_semantics(self):
        self.assertIn(
            "RANK() OVER (ORDER BY min_sell_price_eod ASC NULLS LAST)", self.sql
        )
        self.assertIn(
            "COUNT(*) OVER (PARTITION BY min_sell_price_eod)",
            self.sql,
        )
        self.assertIn("COUNT(min_sell_price_eod) OVER ()", self.sql)
        self.assertIn(
            "RANK() OVER (PARTITION BY category_id ORDER BY "
            "min_sell_price_eod ASC NULLS LAST)",
            self.sql,
        )
        self.assertIn(
            "COUNT(min_sell_price_eod) OVER (PARTITION BY category_id)",
            self.sql,
        )

    def test_popularity_ranks_use_28_day_orders_and_click_windows(self):
        self.assertIn(
            "RANK() OVER (ORDER BY orders_28d DESC NULLS LAST)",
            self.sql,
        )
        self.assertIn("AS popularity_by_orders_neg_rank", self.sql)
        self.assertIn("AS popularity_by_clicks_neg_rank_3d", self.sql)
        self.assertIn("AS popularity_by_clicks_neg_rank_28d", self.sql)
        self.assertNotIn("orders_30d", self.sql)
        self.assertNotIn("clicks_30d", self.sql)

    def test_feedback_rate_uses_global_prior_and_alpha_ten(self):
        self.assertIn("global_feedback_prior AS (", self.sql)
        self.assertIn("SUM(feedback_lte_3)", self.sql)
        self.assertIn("SUM(orders_28d)", self.sql)
        self.assertIn("10.0D * prior.global_feedback_lte_3_to_orders_rate", self.sql)
        self.assertIn("AS feedback_lte_3_to_orders_rate_smoothed", self.sql)
        self.assertIn(
            "AS feedback_lte_3_to_orders_rate_percentile_in_cat",
            self.sql,
        )

    def test_return_baseline_is_weighted_and_relative_rates_are_safe(self):
        for window in query.RETURN_WINDOWS:
            self.assertIn(
                f"SUM(n_returned_{window}d) OVER (PARTITION BY category_id)",
                self.sql,
            )
            self.assertIn(
                f"SUM(n_completed_{window}d + n_returned_{window}d) "
                "OVER (PARTITION BY category_id)",
                self.sql,
            )
            self.assertIn(f"AS return_rate_neg_smoothed_{window}d", self.sql)
            self.assertIn(
                f"AS return_rate_neg_to_category_return_rate_neg_{window}d",
                self.sql,
            )
            self.assertIn(
                f"AS return_rate_neg_smoothed_to_category_return_rate_neg_{window}d",
                self.sql,
            )
        self.assertNotIn("AVG(return_rate", self.sql)
        self.assertNotIn("smothed", self.sql)

    def test_merge_replaces_only_the_requested_snapshot(self):
        merge_sql = query.build_product_ranking_features_merge_query(
            "iceberg.gold.target",
            self.calculated_at,
            self.settings.business_timezone,
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

    def test_dag_waits_for_s1_and_g7_and_disables_alerts(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertEqual(dag_text.count('external_task_id="dq"'), 2)
        self.assertIn("product_metadata", dag_text)
        self.assertIn("product_base_features", dag_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn("feature_namespace: PRODUCT_STATS", config_text)
        self.assertIn('start_date: "2026-09-05T07:00:00Z"', config_text)
        self.assertIn('"recsys"', dag_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)
        self.assertIn("build_feature_stats_task", dag_text)


if __name__ == "__main__":
    unittest.main()
