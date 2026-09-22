import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/product_id/product_cm2_pdp_features/v1"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


query = _load_module("product_cm2_pdp_query", ENTITY / "job/query.py")
runtime_config = _load_module(
    "product_cm2_pdp_runtime_config",
    ENTITY / "job/runtime_config.py",
)
partition = _load_module("product_cm2_pdp_partition", ENTITY / "job/partition.py")


class ProductCm2PdpFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = runtime_config.load_source_settings(ENTITY / "config.yaml")
        cls.calculated_at = datetime(2026, 9, 10, 7, tzinfo=timezone.utc)
        cls.sql = query.build_product_cm2_pdp_features_query(
            cls.settings,
            cls.calculated_at,
        )

    def test_migration_matches_namespaced_features(self):
        migration = (ENTITY / "migrations/create_table.sql").read_text(encoding="utf-8")
        columns = set(
            re.findall(
                r"^\s{4}([A-Za-z][A-Za-z0-9_]*)\s+(?:INT|DOUBLE|TIMESTAMP)\b",
                migration,
                flags=re.MULTILINE,
            )
        )
        self.assertEqual(columns, {"calculated_at", "product_id", *query.FEATURE_COLUMNS})
        self.assertNotIn("sku_id", migration)
        self.assertNotIn("PRODUCT__price", migration)
        self.assertNotIn("BIGINT", migration)

    def test_latest_non_future_s6_and_usd_rate_are_selected(self):
        self.assertIn("SELECT MAX(dt) AS dt", self.sql)
        self.assertIn("WHERE dt <= TIMESTAMP '2026-09-10 12:00:00'", self.sql)
        self.assertIn("currency_name = 'USD'", self.sql)
        self.assertIn(
            "CAST(requested_dt AS TIMESTAMP) <= TIMESTAMP '2026-09-10 12:00:00'",
            self.sql,
        )
        self.assertIn("WHERE row_number = 1", self.sql)

    def test_price_cap_precedes_commission_filter(self):
        self.assertIn("percentile(sell_price_uzs, 0.999D)", self.sql)
        self.assertLess(
            self.sql.index("capped_skus AS"),
            self.sql.index("commissioned_skus AS"),
        )
        self.assertIn("WHERE commission_pct IS NOT NULL", self.sql)
        self.assertNotIn("COALESCE(sku.commission_pct", self.sql)

    def test_pdp_formula_does_not_subtract_main_costs(self):
        self.assertIn("/ 1.12D", self.sql)
        self.assertIn("AS net_inflow_sku", self.sql)
        self.assertNotIn("logistics", self.sql.lower())
        self.assertNotIn("forward_cost", self.sql.lower())
        self.assertNotIn("seller_compensation", self.sql.lower())

    def test_weighted_and_mean_branches_cover_both_values(self):
        self.assertEqual(self.sql.count("WHEN total_orders >= 5"), 2)
        self.assertIn("weighted_net_inflow_sum", self.sql)
        self.assertIn("mean_net_inflow", self.sql)
        self.assertIn("weighted_price_sum", self.sql)
        self.assertIn("mean_price", self.sql)

    def test_cm2_equals_net_inflow_and_rate_is_published(self):
        self.assertIn("net_inflow AS PRODUCT__score", self.sql)
        self.assertIn("net_inflow AS PRODUCT__net_inflow", self.sql)
        self.assertIn("weighted_price AS PRODUCT__weighted_price", self.sql)
        self.assertIn("usd_rate AS PRODUCT__today_rate", self.sql)

    def test_merge_replaces_only_requested_snapshot(self):
        merge_sql = query.build_product_cm2_pdp_features_merge_query(
            "iceberg.gold.target",
            self.calculated_at,
            self.settings.business_timezone,
        )
        self.assertIn("WHEN NOT MATCHED BY SOURCE", merge_sql)
        self.assertIn(
            "target.calculated_at = TIMESTAMP '2026-09-10 12:00:00'",
            merge_sql,
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

    def test_dag_waits_for_s6_and_keeps_alerts_disabled(self):
        dag_text = (ENTITY / "dag.py").read_text(encoding="utf-8")
        config_text = (ENTITY / "config.yaml").read_text(encoding="utf-8")
        self.assertEqual(dag_text.count('external_task_id="dq"'), 1)
        self.assertIn("sku_cm2_inputs_daily", dag_text)
        self.assertIn("execution_date_fn=_daily_s6_logical_date", dag_text)
        self.assertIn("feature_namespace: PRODUCT", config_text)
        self.assertIn("resource_profile: small", config_text)
        self.assertIn('start_date: "2026-09-13T07:00:00Z"', config_text)
        self.assertIn('"recsys"', dag_text)
        self.assertIn("is_paused_upon_creation=True", dag_text)
        self.assertIn('# default_args["on_failure_callback"]', dag_text)
        self.assertEqual(dag_text.count("failure_callback_enabled=False"), 2)
        self.assertIn("build_feature_stats_task", dag_text)

    def test_empty_snapshot_is_rejected_before_merge(self):
        job_text = (ENTITY / "job/getting_product_cm2_pdp_features.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("if not features.take(1)", job_text)
        self.assertIn("features.unpersist()", job_text)


if __name__ == "__main__":
    unittest.main()
