import importlib.util
import sys
from datetime import date
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")


def load_module(name):
    if str(ENTITY_DIR) not in sys.path:
        sys.path.insert(0, str(ENTITY_DIR))
    spec = importlib.util.spec_from_file_location(
        f"ranking_logs_{name}", ENTITY_DIR / "job" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_airflow_timestamp_accepts_supported_formats():
    partition = load_module("partition")

    values = [
        "2026-09-13T12:00:00",
        "2026-09-13T12:00:00+00:00",
        "2026-09-13T12:00:00Z",
        "2026-09-13 12:00:00+00:00",
        "2026-09-13 12:00:00",
    ]
    for value in values:
        parsed = partition.parse_airflow_timestamp(value)
        assert parsed.date().isoformat() == "2026-09-13"
        assert parsed.tzinfo is not None


def test_parse_airflow_timestamp_rejects_garbage():
    partition = load_module("partition")

    try:
        partition.parse_airflow_timestamp("13.09.2026")
    except ValueError:
        return
    raise AssertionError("ожидался ValueError на неподдерживаемом формате")


def test_collection_date_is_the_run_sunday():
    partition = load_module("partition")

    # Ран в воскресенье 2026-09-13 12:00 UTC закрывает окно, начавшееся
    # в воскресенье 2026-09-06 12:00 UTC.
    assert partition.collection_date("2026-09-13 12:00:00+00:00") == date(2026, 9, 13)


def test_event_date_bounds_cover_sunday_to_saturday():
    partition = load_module("partition")

    start, end = partition.event_date_bounds(
        "2026-09-06 12:00:00+00:00", "2026-09-13 12:00:00+00:00"
    )

    assert start == date(2026, 9, 6)
    # Верхняя граница исключительная: последний собираемый день — суббота 12-е.
    assert end == date(2026, 9, 13)
    assert (end - start).days == 7


def test_load_dataset_settings_matches_the_yaml_source_of_truth():
    settings_module = load_module("settings")

    settings = settings_module.load_dataset_settings(ENTITY_DIR / "config.yaml")
    raw = yaml.safe_load((ENTITY_DIR / "config.yaml").read_text(encoding="utf-8"))

    assert settings.model_name == raw["dataset"]["model_name"]
    assert settings.sample_percent == int(raw["dataset"]["sample_percent"])


def test_load_dataset_settings_rejects_out_of_range_percent(tmp_path):
    settings_module = load_module("settings")

    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        "dataset:\n  model_name: m\n  sample_percent: 0\n", encoding="utf-8"
    )

    try:
        settings_module.load_dataset_settings(bad_config)
    except ValueError:
        return
    raise AssertionError("ожидался ValueError на sample_percent вне (0, 100]")
