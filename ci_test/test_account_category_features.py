import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GOLD_ROOT = ROOT / "layers/gold"
EVENT_WINDOWS = (3, 7, 14, 28)
ORDER_WINDOWS = (3, 7, 14, 28, 60, 90)


def _entity(level: int) -> Path:
    return (
        GOLD_ROOT
        / f"account_id_l{level}_category_id"
        / f"account_l{level}_category_features"
        / "v1"
    )


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class AccountCategoryFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.contracts = {}
        for level in range(1, 6):
            entity = _entity(level)
            runtime = _load_module(
                f"account_l{level}_category_runtime",
                entity / "job/runtime_config.py",
            )
            query = _load_module(
                f"account_l{level}_category_query",
                entity / "job/query.py",
            )
            settings = runtime.load_source_settings(entity / "config.yaml")
            sql = query.build_account_category_features_query(
                settings,
                cls.calculated_at,
            )
            cls.contracts[level] = (entity, runtime, query, settings, sql)

    def test_all_five_physical_contracts_have_fixed_windows(self):
        for level, (_, _, _, settings, _) in self.contracts.items():
            with self.subTest(level=level):
                self.assertEqual(settings.category_level, level)
                self.assertEqual(
                    settings.category_column,
                    f"l{level}_category_id",
                )
                self.assertEqual(settings.event_windows_days, EVENT_WINDOWS)
                self.assertEqual(settings.order_windows_days, ORDER_WINDOWS)
                self.assertEqual(
                    settings.successful_order_statuses,
                    ("COMPLETED", "PAID", "DELIVERED", "IN_DELIVERY"),
                )

    def test_migrations_match_generated_feature_contracts(self):
        for level, (entity, _, query, settings, _) in self.contracts.items():
            migration = (entity / "migrations/create_table.sql").read_text(
                encoding="utf-8"
            )
            migration_columns = set(
                re.findall(
                    r"^\s{4}([a-z][a-z0-9_]*)\s+"
                    r"(?:INT|BIGINT|DOUBLE|TIMESTAMP)\b",
                    migration,
                    flags=re.MULTILINE,
                )
            )
            expected = {
                "calculated_at",
                "account_id",
                f"l{level}_category_id",
                *query.feature_columns(settings),
            }
            with self.subTest(level=level):
                self.assertEqual(migration_columns, expected)

    def test_l1_l2_sum_slices_and_l3_l5_deduplicate_full_window(self):
        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertNotIn("n_events", sql)
                if level <= 2:
                    self.assertIn(
                        f"feature_platform_account_l{level}_imp_counts_12h",
                        sql,
                    )
                    self.assertIn(
                        "FROM raw_actions\n)",
                        sql,
                    )
                    self.assertNotIn(
                        "MAX(source_calculated_at) AS source_calculated_at",
                        sql,
                    )
                else:
                    self.assertNotIn("impression_features AS", sql)
                    self.assertIn(
                        "MAX(source_calculated_at) AS source_calculated_at",
                        sql,
                    )
                    self.assertIn(
                        "GROUP BY\n        account_id,\n"
                        "        session_id,\n        product_id,\n"
                        "        event_type",
                        sql,
                    )

    def test_product_categories_use_same_day_s1_snapshot(self):
        for level, (_, _, _, settings, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertIn(
                    "WHERE dt = TIMESTAMP '2026-09-10 00:00:00'",
                    sql,
                )
                self.assertIn(
                    f"AND {settings.category_column} IS NOT NULL",
                    sql,
                )

    def test_orders_keep_lines_but_count_distinct_order_on_category_grain(self):
        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertIn(
                    "order_item.order_item_status IN "
                    "('COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY')",
                    sql,
                )
                self.assertIn("COUNT(DISTINCT CASE", sql)
                self.assertIn("order_item.payment_price AS DOUBLE", sql)
                self.assertIn("order_item.item_quantity AS DOUBLE", sql)
                self.assertNotIn("BETWEEN 1", sql)
                self.assertNotIn("order_item.account_id >", sql)

    def test_l1_l2_publish_all_three_conversion_semantics(self):
        for level in (1, 2):
            _, _, _, _, sql = self.contracts[level]
            prefix = f"l{level}"
            for signal in ("click", "atc", "atf", "order"):
                for window in EVENT_WINDOWS:
                    with self.subTest(
                        level=level,
                        signal=signal,
                        window=window,
                    ):
                        self.assertIn(
                            f"AS {prefix}_account_conv_imp2{signal}_{window}d",
                            sql,
                        )
                        self.assertIn(
                            f"AS {prefix}_conv_imp2{signal}_{window}d",
                            sql,
                        )
                        self.assertIn(
                            f"AS {prefix}_conv_imp2{signal}_vs_account_{window}d",
                            sql,
                        )
            self.assertIn("account_order_features AS", sql)
            self.assertIn(
                "THEN CAST(account_n_orders_3d AS DOUBLE)",
                sql,
            )

        for level in (3, 4, 5):
            _, _, _, _, sql = self.contracts[level]
            self.assertNotIn("_conv_imp2", sql)

    def test_recency_is_only_published_for_l1_l3_l5(self):
        for level, (_, _, query, settings, sql) in self.contracts.items():
            column = f"l{level}_neg_n_days_since_last_click"
            with self.subTest(level=level):
                if level in (1, 3, 5):
                    self.assertIn(f"AS {column}", sql)
                    self.assertIn("/ 86400.0", sql)
                    self.assertIn(column, query.feature_columns(settings))
                else:
                    self.assertNotIn(column, sql)
                    self.assertNotIn(column, query.feature_columns(settings))

    def test_cutoffs_are_half_open_and_snapshot_is_local_time(self):
        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertIn(
                    "TIMESTAMP '2026-09-10 12:00:00' AS calculated_at",
                    sql,
                )
                self.assertIn(
                    "last_received_at < TIMESTAMP '2026-09-10 12:00:00'",
                    sql,
                )
                self.assertIn(
                    "order_item.generated_at < TIMESTAMP '2026-09-10 07:00:00'",
                    sql,
                )

    def test_merge_replaces_only_current_snapshot(self):
        for level, (_, _, query, settings, _) in self.contracts.items():
            merge_sql = query.build_account_category_features_merge_query(
                "iceberg.gold.target",
                settings,
                self.calculated_at,
            )
            with self.subTest(level=level):
                self.assertIn(
                    "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
                    merge_sql,
                )
                self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)
                self.assertIn(
                    f"target.l{level}_category_id = "
                    f"source.l{level}_category_id",
                    merge_sql,
                )

    def test_dags_wait_for_all_repository_managed_sources(self):
        for level, (entity, _, _, _, _) in self.contracts.items():
            dag_text = (entity / "dag.py").read_text(encoding="utf-8")
            config_text = (entity / "config.yaml").read_text(encoding="utf-8")
            with self.subTest(level=level):
                self.assertIn("product_metadata", dag_text)
                self.assertIn(
                    "account_product_session_action_counts_12h",
                    dag_text,
                )
                self.assertIn(
                    "execution_date_fn=_product_metadata_logical_date",
                    dag_text,
                )
                self.assertIn("resource_profile: small", config_text)
                if level <= 2:
                    self.assertIn(
                        f"account_l{level}_imp_counts_12h",
                        dag_text,
                    )


if __name__ == "__main__":
    unittest.main()
