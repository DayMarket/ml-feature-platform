import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_MODULE = (
    ROOT / "layers/silver/sku_id/sku_cm2_inputs_daily/v1/job/runtime.py"
)


def load_runtime():
    spec = importlib.util.spec_from_file_location(
        "test_sku_cm2_inputs_daily_runtime",
        RUNTIME_MODULE,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SkuCm2InputsDailyTimestampTest(unittest.TestCase):
    def test_partition_timestamp_contract(self):
        runtime = load_runtime()

        self.assertEqual(
            runtime.tashkent_date("2026-08-23T19:00:00Z"),
            date(2026, 8, 24),
        )
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            runtime.tashkent_date("not-a-timestamp")


if __name__ == "__main__":
    unittest.main()
