import importlib.util
from pathlib import Path


MODULE_PATH = Path(
    "layers/silver/product_id/product_search_queries/v1/job/partition.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location(
        "product_search_queries_partition",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_airflow_timestamp_accepts_supported_formats():
    module = load_module()

    expected_date = "2026-06-17"
    values = [
        "2026-06-17T00:00:00",
        "2026-06-17T00:00:00+00:00",
        "2026-06-17T00:00:00Z",
        "2026-06-17 00:00:00+00:00",
        "2026-06-17 00:00:00",
    ]

    for value in values:
        assert module.parse_airflow_timestamp(value).date().isoformat() == expected_date
