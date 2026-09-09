import importlib.util
import sys
import unittest
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_MODULE = (
    ROOT / "layers/silver/product_id/product_prices_daily/v1/job/runtime.py"
)


def load_runtime():
    spec = importlib.util.spec_from_file_location(
        "test_product_prices_daily_runtime",
        RUNTIME_MODULE,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ProductPricesDailyTimestampTest(unittest.TestCase):
    def test_partition_timestamp_contract(self):
        runtime = load_runtime()

        self.assertEqual(
            runtime.calculation_tashkent_dt("2026-06-17T19:00:00Z"),
            datetime.fromisoformat("2026-06-18T00:00:00"),
        )
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            runtime.calculation_tashkent_dt("not-a-timestamp")

    def test_eod_source_date_uses_utc_interval_end(self):
        runtime = load_runtime()

        self.assertEqual(
            runtime.source_price_date("2026-09-05T19:00:00Z"),
            date(2026, 9, 4),
        )


if __name__ == "__main__":
    unittest.main()
