import importlib.util
from datetime import date
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "layers/silver/sku_group_id_query_category/sku_group_install/v1/job/partition.py"
)
SPEC = importlib.util.spec_from_file_location("sku_group_install_partition", MODULE_PATH)
partition = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(partition)


@pytest.mark.parametrize(
    "value",
    (
        "2026-06-19T00:00:00+00:00",
        "2026-06-19T00:00:00Z",
        "2026-06-19 00:00:00+00:00",
        "2026-06-19 00:00:00",
    ),
)
def test_parse_partition_date_accepts_airflow_formats(value):
    assert partition.parse_partition_date(value) == date(2026, 6, 19)


@pytest.mark.parametrize("value", ("", "2026/06/19", "not-a-timestamp", None))
def test_parse_partition_date_rejects_unsupported_values(value):
    with pytest.raises(ValueError, match=repr(value)):
        partition.parse_partition_date(value)
