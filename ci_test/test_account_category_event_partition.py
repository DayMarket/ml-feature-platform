import importlib.util
from datetime import date
from pathlib import Path

import pytest


ENTITY_PATHS = (
    "account_l1_event_w_imps_counts",
    "account_l2_event_w_imps_counts",
    "account_l3_event_w_imps_counts",
)


def _load_partition_module(entity_path: str):
    module_path = (
        Path(__file__).resolve().parents[1]
        / "layers/silver/account_id_category_id"
        / entity_path
        / "v1/job/partition.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"{entity_path}_partition",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("entity_path", ENTITY_PATHS)
@pytest.mark.parametrize(
    "value",
    (
        "2026-06-23T00:00:00",
        "2026-06-23T00:00:00+00:00",
        "2026-06-23T00:00:00Z",
        "2026-06-23 00:00:00+00:00",
        "2026-06-23 00:00:00",
    ),
)
def test_parse_partition_date_accepts_airflow_formats(entity_path, value):
    partition = _load_partition_module(entity_path)

    assert partition.parse_partition_date(value) == date(2026, 6, 23)


@pytest.mark.parametrize("entity_path", ENTITY_PATHS)
@pytest.mark.parametrize("value", ("", "2026/06/23", "not-a-timestamp", None))
def test_parse_partition_date_rejects_unsupported_values(entity_path, value):
    partition = _load_partition_module(entity_path)

    with pytest.raises(ValueError, match=repr(value)):
        partition.parse_partition_date(value)
