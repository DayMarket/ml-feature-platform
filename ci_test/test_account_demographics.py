import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_MODULE = (
    ROOT / "layers/silver/account_id/account_demographics/v1/job/runtime.py"
)


def load_runtime():
    spec = importlib.util.spec_from_file_location(
        "test_account_demographics_runtime",
        RUNTIME_MODULE,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class AccountDemographicsTimestampTest(unittest.TestCase):
    def test_partition_timestamp_contract(self):
        runtime = load_runtime()

        self.assertEqual(
            runtime.dt_from_interval_end("2026-08-04T19:00:00Z"),
            date(2026, 8, 5),
        )
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            runtime.dt_from_interval_end("not-a-timestamp")


if __name__ == "__main__":
    unittest.main()
