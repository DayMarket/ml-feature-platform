import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARTITION_MODULE = (
    ROOT
    / "layers/silver/product_id/product_feedback_counts_12h/v1/job/partition.py"
)


def load_partition():
    spec = importlib.util.spec_from_file_location(
        "test_product_feedback_counts_partition",
        PARTITION_MODULE,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ProductFeedbackCountsTimestampTest(unittest.TestCase):
    def test_partition_timestamp_contract(self):
        partition = load_partition()

        self.assertEqual(
            partition.parse_airflow_timestamp("2026-08-06T00:00:00+05:00"),
            datetime(2026, 8, 5, 19, tzinfo=timezone.utc),
        )
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            partition.parse_airflow_timestamp("not-a-timestamp")


if __name__ == "__main__":
    unittest.main()
