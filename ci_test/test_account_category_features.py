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
        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                for window in ORDER_WINDOWS:
                    self.assertIn(f"INTERVAL {window} DAYS", sql)
                self.assertIn(
                    "'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'",
                    sql,
                )

    def test_migrations_match_generated_feature_contracts(self):
        for level, (entity, _, _, _, sql) in self.contracts.items():
            migration = (entity / "migrations/create_table.sql").read_text(
                encoding="utf-8"
            )
            migration_columns = set(
                re.findall(
                    r"^\s{4}([A-Za-z][A-Za-z0-9_]*)\s+"
                    r"(?:INT|BIGINT|DOUBLE|TIMESTAMP)\b",
                    migration,
                    flags=re.MULTILINE,
                )
            )
            with self.subTest(level=level):
                self.assertIn("calculated_at", migration_columns)
                self.assertIn("account_id", migration_columns)
                self.assertIn(f"l{level}_category_id", migration_columns)
                expected = {
                    "calculated_at",
                    "account_id",
                    f"l{level}_category_id",
                    *self.contracts[level][2].FEATURE_COLUMNS,
                }
                self.assertEqual(migration_columns, expected)
                for column in self.contracts[level][2].FEATURE_COLUMNS:
                    self.assertIn(f"AS {column}", sql)
                if level <= 2:
                    for window in EVENT_WINDOWS:
                        self.assertRegex(
                            migration,
                            rf"(?m)^\s+ACCOUNT_L{level}__n_imps_{window}d BIGINT\b",
                        )
                    self.assertIn("AS BIGINT) AS n_imps_28d", self.contracts[level][4])
                else:
                    self.assertNotIn("BIGINT", migration)
                    self.assertNotIn("BIGINT", self.contracts[level][4])

    def test_feature_namespace_prefixes_every_physical_feature_column(self):
        for level, (entity, _, query, _, _) in self.contracts.items():
            config_text = (entity / "config.yaml").read_text(encoding="utf-8")
            migration = (entity / "migrations/create_table.sql").read_text(
                encoding="utf-8"
            )
            with self.subTest(level=level):
                self.assertIn(f"feature_namespace: ACCOUNT_L{level}", config_text)
                self.assertIn(f"ACCOUNT_L{level}__n_clicks_7d", migration)
                self.assertTrue(
                    all(
                        column.startswith(f"ACCOUNT_L{level}__")
                        for column in query.FEATURE_COLUMNS
                    )
                )
                self.assertNotIn(f"l{level}_n_clicks_7d", migration)

    def test_query_topology_is_not_assembled_conditionally(self):
        forbidden_fragments = (
            "ctes.append",
            "if settings.has_impressions",
            "if settings.has_recency",
        )
        for level, (entity, _, _, _, _) in self.contracts.items():
            query_text = (entity / "job/query.py").read_text(encoding="utf-8")
            with self.subTest(level=level):
                for fragment in forbidden_fragments:
                    self.assertNotIn(fragment, query_text)

    def test_all_category_levels_deduplicate_actions_on_full_window(self):
        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertNotIn("n_events", sql)
                if level <= 2:
                    self.assertIn(
                        f"feature_platform_account_l{level}_imp_counts_12h",
                        sql,
                    )
                else:
                    self.assertNotIn("impression_features AS", sql)
                self.assertIn("deduplicated_actions AS", sql)
                self.assertIn(
                    "GROUP BY account_id, session_id, product_id, event_type",
                    sql,
                )
                self.assertNotIn(
                    "MAX(source_calculated_at) AS source_calculated_at",
                    sql,
                )

    def test_product_categories_use_same_day_s1_snapshot(self):
        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertIn(
                    "WHERE dt = TIMESTAMP '2026-09-10 00:00:00'",
                    sql,
                )
                self.assertIn(
                    f"AND l{level}_category_id IS NOT NULL",
                    sql,
                )

    def test_orders_keep_lines_but_count_distinct_order_on_category_grain(self):
        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertIn(
                    "order_item.order_item_status IN (\n"
                    "            'COMPLETED', 'PAID', 'DELIVERED', "
                    "'IN_DELIVERY'\n        )",
                    sql,
                )
                self.assertIn("COUNT(DISTINCT CASE", sql)
                self.assertIn("order_item.payment_price AS DOUBLE", sql)
                self.assertIn("order_item.item_quantity AS DOUBLE", sql)
                self.assertIn("order_item.b2b_order = FALSE", sql)
                self.assertNotIn("BETWEEN 1", sql)
                self.assertNotIn("order_item.account_id >", sql)
                if level <= 2:
                    self.assertNotIn("CAST(order_item.order_id AS INT)", sql)
                    self.assertNotIn("CAST(order_item.sku_id AS INT)", sql)

    def test_cross_slice_session_product_distinct_semantics(self):
        rows = (
            ("2026-09-09T12:00:00", 7, "session-1", 101, "PRODUCT_VIEW"),
            ("2026-09-10T00:00:00", 7, "session-1", 101, "PRODUCT_VIEW"),
        )
        full_window_count = len({row[1:] for row in rows})
        self.assertEqual(full_window_count, 1)

        for level, (_, _, _, _, sql) in self.contracts.items():
            with self.subTest(level=level):
                self.assertIn("deduplicated_actions AS", sql)
                self.assertIn(
                    "GROUP BY account_id, session_id, product_id, event_type",
                    sql,
                )

    def test_l1_l2_publish_raw_and_relative_conversion_semantics(self):
        for level in (1, 2):
            _, _, _, _, sql = self.contracts[level]
            for signal in ("click", "atc", "atf", "order"):
                for window in EVENT_WINDOWS:
                    with self.subTest(
                        level=level,
                        signal=signal,
                        window=window,
                    ):
                        self.assertIn(
                            f"AS conv_imp2{signal}_raw_{window}d",
                            sql,
                        )
                        self.assertIn(
                            f"account.{signal}_{window}d AS "
                            f"total_account_conv_imp2{signal}_raw_{window}d",
                            sql,
                        )
                        self.assertIn(
                            f"AS conv_imp2{signal}_div_total_category_conv_{window}d",
                            sql,
                        )
                        self.assertIn(
                            f"AS conv_imp2{signal}_div_total_account_conv_{window}d",
                            sql,
                        )
            self.assertIn("account_order_features AS", sql)
            self.assertIn(
                "MAX(account_orders.account_n_orders_3d)",
                sql,
            )

        for level in (3, 4, 5):
            _, _, _, _, sql = self.contracts[level]
            self.assertNotIn("_conv_imp2", sql)

    def test_recency_is_only_published_for_l1_l3_l5(self):
        for level, (entity, _, _, _, sql) in self.contracts.items():
            column = "neg_n_days_since_last_click"
            interval_column = "n_days_between_last_purchase_and_last_click"
            migration = (entity / "migrations/create_table.sql").read_text(
                encoding="utf-8"
            )
            with self.subTest(level=level):
                if level in (1, 3, 5):
                    self.assertIn(f"AS {column}", sql)
                    self.assertIn("/ 86400.0", sql)
                    self.assertNotIn("CEIL(", sql)
                    self.assertIn(column, migration)
                    self.assertRegex(
                        migration,
                        rf"(?m)^\s+ACCOUNT_L{level}__{column} DOUBLE\b",
                    )
                    self.assertIn("MAX(generated_at) AS last_purchase_at", sql)
                    self.assertIn(f"AS {interval_column}", sql)
                    self.assertIn(interval_column, migration)
                    self.assertRegex(
                        migration,
                        rf"(?m)^\s+ACCOUNT_L{level}__{interval_column} DOUBLE\b",
                    )
                    interval_line = next(
                        line
                        for line in sql.splitlines()
                        if f"AS {interval_column}" in line
                    )
                    self.assertNotIn("COALESCE", interval_line)
                else:
                    self.assertNotIn(column, sql)
                    self.assertNotIn(column, migration)
                    self.assertNotIn(interval_column, sql)
                    self.assertNotIn(interval_column, migration)

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
                    f"target.l{level}_category_id = source.l{level}_category_id",
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
                self.assertIn(
                    'start_date: "2026-09-05T07:00:00Z"',
                    config_text,
                )
                self.assertIn("severity: P3", config_text)
                self.assertIn(
                    "oncall_webhook_conn_id: oncall_webhook_recsys",
                    config_text,
                )
                self.assertIn("is_paused_upon_creation=True", dag_text)
                self.assertIn('# default_args["on_failure_callback"]', dag_text)
                self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)
                if level <= 2:
                    self.assertIn(
                        f"account_l{level}_imp_counts_12h",
                        dag_text,
                    )

    def test_l1_l2_dq_rejects_non_finite_conversions(self):
        for level in (1, 2):
            entity = self.contracts[level][0]
            config_text = (entity / "config.yaml").read_text(encoding="utf-8")
            with self.subTest(level=level):
                self.assertIn("- name: finite", config_text)
                self.assertIn(
                    f"- ACCOUNT_L{level}__conv_imp2click_raw_3d",
                    config_text,
                )
                self.assertIn(
                    f"- ACCOUNT_L{level}__total_account_conv_imp2click_raw_3d",
                    config_text,
                )
                self.assertIn(
                    f"- ACCOUNT_L{level}__conv_imp2click_div_total_category_conv_3d",
                    config_text,
                )
                self.assertIn(
                    f"- ACCOUNT_L{level}__conv_imp2order_div_total_account_conv_28d",
                    config_text,
                )

    def test_recency_contracts_check_relative_max_per_account(self):
        for level in (1, 3, 5):
            entity = self.contracts[level][0]
            config_text = (entity / "config.yaml").read_text(encoding="utf-8")
            with self.subTest(level=level):
                self.assertIn("- name: group_max_equals", config_text)
                self.assertIn(
                    f"column: ACCOUNT_L{level}__neg_n_days_since_last_click_rel",
                    config_text,
                )
                self.assertIn("group_by: [account_id]", config_text)


if __name__ == "__main__":
    unittest.main()
