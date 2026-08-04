import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUERY_PATH = (
    ROOT
    / "layers"
    / "silver"
    / "sku_id_promotion_id"
    / "dynamic_pricing_prices"
    / "v1"
    / "job"
    / "query.py"
)


def load_query():
    spec = importlib.util.spec_from_file_location(
        "test_dynamic_pricing_silver_query_module",
        QUERY_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["test_dynamic_pricing_silver_query_module"] = module
    spec.loader.exec_module(module)
    return module


class DynamicPricingSilverQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_query()

    def test_query_filters_dynamic_pricing_prefix(self):
        sql = self.query.build_query(date(2026, 7, 1))

        self.assertIn("starts_with(promotion_id, 'dyno_pricing_')", sql)
        self.assertNotIn("promotion_filter", sql)


if __name__ == "__main__":
    unittest.main()
