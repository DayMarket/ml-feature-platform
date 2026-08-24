import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARTITION_MODULES = (
    ROOT
    / "layers/silver/account_id_l1_category_id"
    / "account_l1_impression_counts_12h/v1/job/partition.py",
    ROOT
    / "layers/silver/account_id_l2_category_id"
    / "account_l2_impression_counts_12h/v1/job/partition.py",
    ROOT
    / "layers/silver/account_id_session_id_product_id_event_type"
    / "account_product_session_action_counts_12h/v1/job/partition.py",
)


def _load_partition_module(path: Path, index: int):
    spec = importlib.util.spec_from_file_location(f"s2_partition_{index}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class S2PartitionTimestampTest(unittest.TestCase):
    def test_partition_timestamp_contract(self):
        for index, path in enumerate(PARTITION_MODULES):
            partition = _load_partition_module(path, index)
            self.assertEqual(
                partition.parse_airflow_timestamp(
                    "2026-08-06T00:00:00+05:00"
                ),
                datetime(2026, 8, 5, 19, tzinfo=timezone.utc),
            )
            with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
                partition.parse_airflow_timestamp("not-a-timestamp")


if __name__ == "__main__":
    unittest.main()
