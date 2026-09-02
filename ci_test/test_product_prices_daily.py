import importlib.util
import sys
import unittest
from datetime import datetime
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
            datetime(2026, 6, 18, 0, 0),
        )
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            runtime.calculation_tashkent_dt("not-a-timestamp")


if __name__ == "__main__":
    unittest.main()
