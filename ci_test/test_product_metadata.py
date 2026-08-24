import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARTITION_MODULE = (
    ROOT / "layers/silver/product_id/product_metadata/v1/job/partition.py"
)


def _load_partition_module():
    spec = importlib.util.spec_from_file_location(
        "product_metadata_partition",
        PARTITION_MODULE,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ProductMetadataTimestampTest(unittest.TestCase):
    def test_partition_timestamp_contract(self):
        partition = _load_partition_module()

        self.assertEqual(
            partition.dt_from_partition_end("2026-08-04T19:00:00Z"),
            date(2026, 8, 5),
        )
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            partition.dt_from_partition_end("not-a-timestamp")


if __name__ == "__main__":
    unittest.main()
