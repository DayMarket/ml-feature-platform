import importlib.util
import sys
import unittest
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_MODULE = (
    ROOT / "layers/silver/sku_id/sku_cm2_inputs_daily/v1/job/runtime.py"
)
QUERY_MODULE = ROOT / "layers/silver/sku_id/sku_cm2_inputs_daily/v1/job/query.py"


def load_runtime():
    spec = importlib.util.spec_from_file_location(
        "test_sku_cm2_inputs_daily_runtime",
        RUNTIME_MODULE,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_query():
    spec = importlib.util.spec_from_file_location(
        "test_sku_cm2_inputs_daily_query",
        QUERY_MODULE,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SkuCm2InputsDailyTimestampTest(unittest.TestCase):
    def test_partition_timestamp_contract(self):
        runtime = load_runtime()

        self.assertEqual(
            runtime.tashkent_dt("2026-08-23T19:00:00Z"),
            datetime.fromisoformat("2026-08-24T00:00:00"),
        )
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            runtime.tashkent_dt("not-a-timestamp")

    def test_eod_source_date_uses_utc_interval_end(self):
        query = load_query()

        self.assertEqual(
            query.source_price_date(
                datetime.fromisoformat("2026-09-04T19:00:00+00:00")
            ),
            date(2026, 9, 3),
        )


if __name__ == "__main__":
    unittest.main()
