import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers" / "silver" / "sku_id" / "sku_daily_dynamic_prices" / "v1"


def load_module(filename, module_name):
    spec = importlib.util.spec_from_file_location(
        module_name,
        ENTITY / "job" / filename,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class SkuDailyDynamicPricesQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_module("query.py", "test_sku_daily_dynamic_prices_query_module")

    def test_window_is_the_closed_utc_day(self):
        _, params = self.query.build_query(date(2026, 8, 19))

        self.assertEqual(params["partition_date"], "2026-08-19")
        self.assertEqual(params["window_start"], "2026-08-19 00:00:00")
        self.assertEqual(params["window_end"], "2026-08-20 00:00:00")

    def test_query_is_parameterized_not_interpolated(self):
        sql, _ = self.query.build_query(date(2026, 8, 19))

        self.assertNotIn("2026-08-19", sql)
        self.assertIn("%(window_start)s", sql)
        self.assertIn("%(window_end)s", sql)
        self.assertIn("%(partition_date)s", sql)

    def test_window_literals_pin_utc(self):
        # Серверная таймзона ClickHouse — Asia/Tashkent; без явной зоны сутки уезжают на 5 часов.
        sql, _ = self.query.build_query(date(2026, 8, 19))

        self.assertIn("toDateTime(%(window_start)s, 'UTC')", sql)
        self.assertIn("toDateTime(%(window_end)s, 'UTC')", sql)

    def test_aggregates_over_every_promotion_id(self):
        sql, _ = self.query.build_query(date(2026, 8, 19))

        self.assertNotIn("promotion_id =", sql)
        self.assertNotIn("starts_with(promotion_id", sql)

    def test_mode_is_exact(self):
        sql, _ = self.query.build_query(date(2026, 8, 19))

        self.assertNotIn("topK", sql)
        self.assertIn("argMax(dp_sell_price, (cnt, last_seen))", sql)
        self.assertIn("argMax(seller_price, (cnt, last_seen))", sql)

    def test_final_price_subtracts_the_dynamic_discount(self):
        sql, _ = self.query.build_query(date(2026, 8, 19))

        self.assertIn("calculated_for_price - discount_amount", sql)
        self.assertIn("pricing.dynamic_discount", sql)


class PartitionDateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_module(
            "runtime.py",
            "test_sku_daily_dynamic_prices_runtime_module",
        )

    def test_partition_is_the_previous_utc_day(self):
        accepted = (
            "2026-08-20T01:00:00",
            "2026-08-20T01:00:00+00:00",
            "2026-08-20T01:00:00Z",
            "2026-08-20 01:00:00+00:00",
            "2026-08-20 01:00:00",
        )
        for value in accepted:
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.previous_utc_date(value),
                    date(2026, 8, 19),
                )

    def test_unsupported_value_names_itself(self):
        with self.assertRaises(ValueError) as raised:
            self.runtime.previous_utc_date("20/08/2026")

        self.assertIn("20/08/2026", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
