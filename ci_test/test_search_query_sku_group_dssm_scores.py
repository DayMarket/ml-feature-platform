import importlib.util
from pathlib import Path


MODULE_PATH = Path(
    "layers/silver/query_sku_group_id/"
    "search_query_sku_group_dssm_scores/v1/job/partition.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location(
        "search_query_sku_group_dssm_scores_partition",
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


def test_utc_day_bounds_use_calendar_day_for_non_midnight_schedule():
    module = load_module()

    day_start, day_end = module.utc_day_bounds_from_interval_start(
        "2026-06-17 02:00:00+00:00"
    )

    assert day_start.isoformat() == "2026-06-17T00:00:00+00:00"
    assert day_end.isoformat() == "2026-06-18T00:00:00+00:00"
