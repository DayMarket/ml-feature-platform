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

    def test_output_promotion_ids_appends_default_once(self):
        self.assertEqual(
            self.query.output_promotion_ids(["model_a"]),
            ["model_a", self.query.DEFAULT_PROMOTION_ID],
        )
        self.assertEqual(
            self.query.output_promotion_ids(["model_a", "0"]),
            ["model_a", "0"],
        )

    def test_gold_query_keeps_default_promotion_out_of_discount_sources(self):
        sql = self.query.build_gold_query(
            calculated_at=datetime(2026, 7, 1, 3, 0, 0),
            promotion_ids=[self.query.DEFAULT_PROMOTION_ID],
            silver_table='"dwh-iceberg".silver.feature_platform_dynamic_pricing_daily_prices',
            history_days=15,
        )

        self.assertIn("('0')", sql)
        self.assertIn("CROSS JOIN promotion_filter pf", sql)
        self.assertIn("WHERE promotion_id <> '0'", sql)
        self.assertNotIn(
            "promotion_id IN (SELECT promotion_id FROM promotion_filter)",
            sql,
        )
        self.assertEqual(
            sql.count(
                "promotion_id IN (SELECT promotion_id FROM source_promotion_filter)"
            ),
            2,
        )

    def test_today_discounts_query_uses_source_promotion_filter(self):
        sql = self.query.build_today_discounts_query(
            calculated_at=datetime(2026, 7, 1, 3, 0, 0),
            promotion_ids=[self.query.DEFAULT_PROMOTION_ID],
        )

        self.assertIn("('0')", sql)
        self.assertIn("WHERE promotion_id <> '0'", sql)
        self.assertIn(
            "promotion_id IN (SELECT promotion_id FROM source_promotion_filter)",
            sql,
        )


if __name__ == "__main__":
    unittest.main()
