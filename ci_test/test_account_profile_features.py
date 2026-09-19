import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/account_id/account_profile_features/v1"
ORDER_WINDOWS = (7, 28, 90)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("account_profile_features_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "account_profile_features_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module(
    "account_profile_features_partition",
    ENTITY / "job/partition.py",
)


class AccountProfileFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_account_profile_features_query(
            cls.settings,
            cls.calculated_at,
        )

    def test_feature_contract_and_windows(self):
        self.assertEqual(len(query.DEMOGRAPHIC_COLUMNS), 6)
        self.assertEqual(len(query.ORDER_FEATURE_COLUMNS), 72)
        self.assertEqual(len(query.LAST_CLICKED_COLUMNS), 14)
        self.assertEqual(len(query.FEATURE_COLUMNS), 92)
        self.assertEqual(len(query.FEATURE_COLUMNS), len(set(query.FEATURE_COLUMNS)))

        for window in ORDER_WINDOWS:
            self.assertIn(
                f"ACCOUNT__median_order_total_{window}d",
                query.FEATURE_COLUMNS,
            )
            self.assertIn(
                f"ACCOUNT__last_purchased_neg_p90_popularity_rank_{window}d",
                query.FEATURE_COLUMNS,
            )
            self.assertIn(
                f"ACCOUNT__last_purchased_neg_p10_popularity_rank_{window}d",
                query.FEATURE_COLUMNS,
            )
            self.assertIn(f"INTERVAL {window} DAYS", self.sql)
        self.assertNotRegex(self.sql, r"INTERVAL (?:30|63|91) DAYS")

    def test_migration_matches_generated_feature_contract(self):
        migration = (ENTITY / "migrations/create_table.sql").read_text(encoding="utf-8")
        migration_columns = set(
            re.findall(
                r"^\s{4}([A-Za-z][A-Za-z0-9_]*)\s+(?:INT|DOUBLE|STRING|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        expected = {"calculated_at", "account_id", *query.FEATURE_COLUMNS}
        self.assertEqual(migration_columns, expected)
        self.assertNotIn("BIGINT", migration)
        self.assertIn("TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')", migration)

    def test_namespace_schedule_and_resources(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn("feature_namespace: ACCOUNT", config_text)
        self.assertTrue(
            all(
                column.startswith("ACCOUNT__")
                for column in query.FEATURE_COLUMNS
            )
        )
        self.assertIn("AS ACCOUNT__gender", self.sql)
        self.assertIn("primary_key: calculated_at,account_id", config_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn('schedule: "0 7,19 * * *"', config_text)
        self.assertIn('start_date: "2026-09-05T07:00:00Z"', config_text)

    def test_query_has_explicit_topology(self):
        query_text = (ENTITY / "job/query.py").read_text(encoding="utf-8")
        self.assertNotIn("ctes.append", query_text)
        self.assertNotIn("has_impressions", query_text)
        self.assertIn("WITH demographics AS (", self.sql)
        self.assertIn("order_profile AS (", self.sql)
        self.assertIn("last_clicked_profile AS (", self.sql)
        self.assertIn("account_population AS (", self.sql)

    def test_daily_snapshots_use_their_actual_timestamp_conventions(self):
        self.assertIn(
            "FROM iceberg.silver.feature_platform_account_demographics\n"
            "    WHERE dt = TIMESTAMP '2026-09-10 00:00:00'",
            self.sql,
        )
        self.assertIn(
            "FROM iceberg.silver.feature_platform_product_prices_daily\n"
            "    WHERE dt = TIMESTAMP '2026-09-10 00:00:00'",
            self.sql,
        )
        self.assertIn(
            "FROM iceberg.silver.feature_platform_product_metadata\n"
            "    WHERE dt = TIMESTAMP '2026-09-09 19:00:00'",
            self.sql,
        )

    def test_order_profile_uses_required_filters_and_weights(self):
        self.assertIn("LEFT JOIN sku_mapping sku", self.sql)
        self.assertIn("ON order_item.sku_id = sku.sku_id", self.sql)
        self.assertIn(
            "'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'",
            self.sql,
        )
        self.assertIn("order_item.b2b_order = FALSE", self.sql)
        self.assertIn("GROUP BY account_id, order_id", self.sql)
        self.assertIn("CAST(SUM(line_gmv) AS DOUBLE) AS order_total", self.sql)
        self.assertIn("THEN item_quantity END", self.sql)
        self.assertIn("COUNT(DISTINCT CASE", self.sql)
        self.assertNotIn("BETWEEN 1", self.sql)
        self.assertNotIn("MAX_INT_ID", self.sql)

    def test_order_attributes_are_joined_at_snapshot_time(self):
        for table in (
            "feature_platform_category_gender_features",
            "feature_platform_product_base_features",
            "feature_platform_product_ranking_features",
        ):
            self.assertIn(f"FROM iceberg.gold.{table}", self.sql)
        self.assertGreaterEqual(
            self.sql.count("calculated_at = TIMESTAMP '2026-09-10 12:00:00'"),
            3,
        )
        self.assertIn("base.discount", self.sql)
        self.assertIn("base.rating", self.sql)
        self.assertIn("category.category_gender", self.sql)
        self.assertIn(
            "CATEGORY_DEMOGRAPHICS__gender AS category_gender",
            self.sql,
        )
        self.assertIn(
            "PRODUCT__discount AS discount",
            self.sql,
        )
        self.assertIn(
            "PRODUCT__rating AS rating",
            self.sql,
        )
        self.assertIn(
            "PRODUCT_RANKING__popularity_by_orders_neg_rank\n"
            "            AS popularity_by_orders_neg_rank",
            self.sql,
        )
        self.assertIn(
            "PRODUCT_RANKING__popularity_by_orders_neg_rank_in_cat\n"
            "            AS popularity_by_orders_neg_rank_in_cat",
            self.sql,
        )

    def test_popularity_percentile_direction_is_explicit(self):
        self.assertIn(
            "popularity_by_orders_neg_rank END, 0.1) AS "
            "last_purchased_neg_p90_popularity_rank_7d",
            self.sql,
        )
        self.assertIn(
            "popularity_by_orders_neg_rank END, 0.9) AS "
            "last_purchased_neg_p10_popularity_rank_7d",
            self.sql,
        )

    def test_last_clicked_profile_deduplicates_and_limits_before_price_filter(self):
        self.assertIn(
            "GROUP BY account_id, session_id, product_id",
            self.sql,
        )
        self.assertIn("MAX(last_received_at) AS last_received_at", self.sql)
        self.assertIn("WHERE click_row_number <= 75", self.sql)
        self.assertLess(
            self.sql.index("WHERE click_row_number <= 75"),
            self.sql.index("WHERE prices.min_sell_price_eod IS NOT NULL"),
        )
        self.assertIn("event_type = 'PRODUCT_VIEW'", self.sql)
        self.assertNotIn("n_events", self.sql)

    def test_last_clicked_price_percentile_uses_average_rank(self):
        self.assertIn(
            "RANK() OVER (ORDER BY last_clicked_avg_price ASC NULLS LAST)",
            self.sql,
        )
        self.assertIn(
            "COUNT(*) OVER (PARTITION BY last_clicked_avg_price)",
            self.sql,
        )
        self.assertIn(
            "(CAST(last_clicked_avg_price_tie_count AS DOUBLE) - 1.0D) / 2.0D",
            self.sql,
        )
        self.assertIn("COUNT(last_clicked_avg_price) OVER ()", self.sql)

    def test_population_is_union_of_three_blocks_and_missing_blocks_stay_null(self):
        population = self.sql[self.sql.index("account_population AS (") :]
        self.assertIn("SELECT account_id FROM demographics", population)
        self.assertIn("SELECT account_id FROM order_profile", population)
        self.assertIn("SELECT account_id FROM last_clicked_profile", population)
        self.assertNotIn("COALESCE(", population)

    def test_cutoffs_and_output_snapshot(self):
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
        merge_sql = query.build_account_profile_features_merge_query(
            "iceberg.gold.target",
            self.settings,
            self.calculated_at,
        )
        self.assertIn(
            "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            merge_sql,
        )
        self.assertIn("target.account_id = source.account_id", merge_sql)
        self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)

    def test_dag_waits_for_all_repository_dependencies_without_active_alerts(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        self.assertEqual(dag_text.count('external_task_id="dq"'), 7)
        for dependency in (
            "product_id.product_metadata",
            "product_id.product_prices_daily",
            "account_id.account_demographics",
            "account_product_session_action_counts_12h",
            "category_gender_features",
            "product_base_features",
            "product_ranking_features",
        ):
            self.assertIn(dependency, dag_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)

    def test_dq_covers_requested_domains(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        for value in ("MALE", "FEMALE", "UNKNOWN", "IOS", "ANDROID", "WEB"):
            self.assertIn(value, config_text)
        for column in (
            "last_clicked_avg_price_pctl",
            "last_clicked_female_cat_share_among_gendered",
            "last_purchased_null_rating_share_90d",
            "last_purchased_unisex_cat_share_28d",
        ):
            self.assertIn(f"column: ACCOUNT__{column}", config_text)
        self.assertIn(
            "ACCOUNT__min_order_total_28d <= "
            "ACCOUNT__median_order_total_28d",
            config_text,
        )


if __name__ == "__main__":
    unittest.main()
