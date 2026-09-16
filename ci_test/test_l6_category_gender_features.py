import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = (
    ROOT
    / "layers/gold/l6_category_id/l6_category_gender_features/v1"
)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("l6_category_gender_features_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "l6_category_gender_features_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module(
    "l6_category_gender_features_partition",
    ENTITY / "job/partition.py",
)


class L6CategoryGenderFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_l6_category_gender_features_query(
            cls.settings,
            cls.calculated_at,
        )

    def test_feature_contract(self):
        self.assertEqual(
            query.FEATURE_COLUMNS,
            (
                "category_female_product_session_share_28d",
                "category_male_product_session_share_28d",
                "n_unique_known_gender_clickers_28d",
                "n_unique_female_clickers_28d",
                "n_unique_male_clickers_28d",
                "category_gender",
            ),
        )

    def test_migration_matches_feature_contract_and_uses_int(self):
        migration = (ENTITY / "migrations/create_table.sql").read_text(
            encoding="utf-8"
        )
        migration_columns = set(
            re.findall(
                r"^\s{4}([a-z][a-z0-9_]*)\s+(?:INT|DOUBLE|STRING|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        expected = {"calculated_at", "l6_category_id", *query.FEATURE_COLUMNS}
        self.assertEqual(migration_columns, expected)
        self.assertNotIn("BIGINT", migration)
        self.assertIn("TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')", migration)

    def test_namespace_schedule_resources_and_start_date(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn("feature_namespace: L6_CATEGORY_GENDER", config_text)
        self.assertIn("primary_key: calculated_at,l6_category_id", config_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn('schedule: "0 7,19 * * *"', config_text)
        self.assertIn('start_date: "2026-09-05T07:00:00Z"', config_text)
        self.assertIn("lookback_days: 28", config_text)

    def test_query_has_static_cte_topology(self):
        query_text = (ENTITY / "job/query.py").read_text(encoding="utf-8")
        self.assertNotIn("ctes.append", query_text)
        for cte in (
            "product_metadata",
            "demographics",
            "category_genders",
            "deduplicated_product_views",
            "enriched_product_views",
            "category_statistics",
        ):
            self.assertIn(f"{cte} AS (", self.sql)

    def test_product_views_use_exact_half_open_28_day_window(self):
        self.assertIn("event_type = 'PRODUCT_VIEW'", self.sql)
        self.assertIn(
            "calculated_at > TIMESTAMP '2026-09-10 12:00:00'\n"
            "            - INTERVAL 28 DAYS",
            self.sql,
        )
        self.assertIn(
            "calculated_at <= TIMESTAMP '2026-09-10 12:00:00'",
            self.sql,
        )
        self.assertIn(
            "last_received_at >= TIMESTAMP '2026-09-10 12:00:00'\n"
            "            - INTERVAL 28 DAYS",
            self.sql,
        )
        self.assertIn(
            "last_received_at < TIMESTAMP '2026-09-10 12:00:00'",
            self.sql,
        )

    def test_product_session_rows_are_deduplicated_on_full_window(self):
        self.assertIn("MAX(last_received_at) AS last_received_at", self.sql)
        self.assertIn(
            "GROUP BY\n        account_id,\n        session_id,\n        product_id",
            self.sql,
        )
        self.assertNotIn("n_events", self.sql)

    def test_daily_snapshots_use_actual_timestamp_conventions(self):
        self.assertIn(
            "FROM iceberg.silver.feature_platform_product_metadata\n"
            "    WHERE dt = TIMESTAMP '2026-09-09 19:00:00'",
            self.sql,
        )
        self.assertIn(
            "FROM iceberg.silver.feature_platform_account_demographics\n"
            "    WHERE dt = TIMESTAMP '2026-09-10 00:00:00'",
            self.sql,
        )
        self.assertIn(
            "TIMESTAMP '2026-09-10 12:00:00' AS calculated_at",
            self.sql,
        )

    def test_l6_and_account_gender_joins_keep_unknown_gender_views(self):
        self.assertIn("INNER JOIN product_metadata metadata", self.sql)
        self.assertIn("WHERE metadata.l6_category_id IS NOT NULL", self.sql)
        self.assertIn("LEFT JOIN demographics", self.sql)
        self.assertNotIn("account_id BETWEEN", self.sql)
        self.assertNotIn("MAX_INT_ID", self.sql)

    def test_l6_gender_comes_from_direct_category_mapping_after_aggregation(self):
        self.assertIn("dominant_gender IN ('M', 'F', 'U')", self.sql)
        self.assertIn("CAST(category_id AS INT) AS l6_category_id", self.sql)
        self.assertIn(
            "LEFT JOIN category_genders genders\n"
            "    ON statistics.l6_category_id = genders.l6_category_id",
            self.sql,
        )
        self.assertGreater(
            self.sql.index("FROM category_statistics statistics"),
            self.sql.index("category_statistics AS ("),
        )

    def test_shares_and_unique_counts_use_documented_gender_semantics(self):
        self.assertIn(
            "SUM(CASE WHEN account_gender = 'FEMALE' THEN 1 ELSE 0 END)",
            self.sql,
        )
        self.assertIn(
            "SUM(CASE WHEN account_gender = 'MALE' THEN 1 ELSE 0 END)",
            self.sql,
        )
        self.assertIn("account_gender IN ('MALE', 'FEMALE')", self.sql)
        self.assertIn("COUNT(DISTINCT CASE WHEN account_gender = 'FEMALE'", self.sql)
        self.assertIn("COUNT(DISTINCT CASE WHEN account_gender = 'MALE'", self.sql)
        self.assertIn("NULLIF(", self.sql)

    def test_candidate_enrichment_is_not_materialized(self):
        for column in (
            "account_gender_category_click_share_28d",
            "account_gender_mismatch_category",
            "account_category_female_click_share_abs_diff_28d",
        ):
            self.assertNotIn(column, query.FEATURE_COLUMNS)
            self.assertNotIn(column, self.sql)

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
        merge_sql = query.build_l6_category_gender_features_merge_query(
            "iceberg.gold.target",
            self.settings,
            self.calculated_at,
        )
        self.assertIn(
            "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            merge_sql,
        )
        self.assertIn(
            "target.l6_category_id = source.l6_category_id",
            merge_sql,
        )
        self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)

    def test_dag_waits_for_repository_sources_and_disables_alert_callbacks(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        self.assertEqual(dag_text.count('external_task_id="dq"'), 3)
        for dependency in (
            "product_id.product_metadata",
            "account_product_session_action_counts_12h",
            "account_id.account_demographics",
        ):
            self.assertIn(dependency, dag_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)

    def test_dq_covers_gender_domains_shares_and_count_invariants(self):
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertIn("values: [M, F, U]", config_text)
        self.assertIn(
            "n_unique_known_gender_clickers_28d = "
            "n_unique_female_clickers_28d + n_unique_male_clickers_28d",
            config_text,
        )
        self.assertIn(
            "category_female_product_session_share_28d + "
            "category_male_product_session_share_28d - 1.0",
            config_text,
        )


if __name__ == "__main__":
    unittest.main()
