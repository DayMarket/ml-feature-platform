import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUERY_PATH = (
    ROOT
    / "layers"
    / "gold"
    / "calculated_at_sku_id_promotion_id"
    / "dynamic_pricing_price_features"
    / "v1"
    / "job"
    / "query.py"
)


def load_query():
    spec = importlib.util.spec_from_file_location(
        "test_dynamic_pricing_price_query",
        QUERY_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["test_dynamic_pricing_price_query"] = module
    spec.loader.exec_module(module)
    return module


class DynamicPricingDefaultPromotionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_query()

    def test_promotion_discovery_filters_prefix_in_both_sources(self):
        sql = self.query.build_promotion_ids_query(
            silver_table='"dwh-iceberg".silver.feature_platform_dynamic_pricing_daily_prices',
            history_days=15,
        )

        self.assertEqual(
            sql.count("starts_with(promotion_id, 'dyno_pricing_')"),
            2,
        )
        self.assertIn("SELECT DISTINCT promotion_id", sql)

    def test_gold_query_keeps_default_promotion_out_of_discount_sources(self):
        sql = self.query.build_gold_query(
            calculated_at=datetime(2026, 7, 1, 3, 0, 0),
            promotion_id=self.query.DEFAULT_PROMOTION_ID,
            silver_table='"dwh-iceberg".silver.feature_platform_dynamic_pricing_daily_prices',
            history_days=15,
        )

        self.assertIn("'0' AS promotion_id", sql)
        self.assertEqual(sql.count("AND FALSE"), 2)
        self.assertEqual(
            sql.count("starts_with(promotion_id, 'dyno_pricing_')"),
            2,
        )

    def test_today_discounts_query_uses_prefix_filter(self):
        sql = self.query.build_today_discounts_query(
            calculated_at=datetime(2026, 7, 1, 3, 0, 0),
            promotion_id="dyno_pricing_model_a",
        )

        self.assertIn("promotion_id = 'dyno_pricing_model_a'", sql)
        self.assertIn("starts_with(promotion_id, 'dyno_pricing_')", sql)

    def test_rejects_non_dynamic_pricing_model(self):
        with self.assertRaisesRegex(ValueError, "must start"):
            self.query.build_gold_query(
                calculated_at=datetime(2026, 7, 1, 3, 0, 0),
                promotion_id="model_a",
                silver_table='"dwh-iceberg".silver.feature_platform_dynamic_pricing_daily_prices',
                history_days=15,
            )


if __name__ == "__main__":
    unittest.main()
