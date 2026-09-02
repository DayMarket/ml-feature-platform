import contextlib
import importlib.util
import sys
from datetime import date
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")


@contextlib.contextmanager
def _isolated_job_package():
    """Имя пакета `job` занято десятком энтити репозитория, и соседний тест
    (ci_test/test_query_id_features.py) кэширует в sys.modules своё
    `job.entities` без уборки. Снимаем чужой кэш на время загрузки и
    возвращаем его на место, чтобы не сломать ни себя, ни соседей."""
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "job" or name.startswith("job.")
    }
    for name in saved:
        del sys.modules[name]
    saved_path = list(sys.path)
    sys.path.insert(0, str(ENTITY_DIR))
    try:
        yield
    finally:
        sys.path[:] = saved_path
        for name in [n for n in sys.modules if n == "job" or n.startswith("job.")]:
            del sys.modules[name]
        sys.modules.update(saved)


def load_module(name):
    with _isolated_job_package():
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


def test_load_settings_survives_poisoned_cache():
    """Регрессионный тест: проверяет, что наш загрузчик не зависит от кэша
    других энтити. Сосед ci_test/test_query_id_features.py кэширует свой
    job.entities в sys.modules без уборки, и это должно нас не ломать."""
    # Отравим кэш фиктивным модулем, который не имеет DatasetSettings
    fake_job = type(sys)("job")
    fake_entities = type(sys)("job.entities")
    fake_entities.SomeOtherClass = type("SomeOtherClass", (), {})
    fake_job.entities = fake_entities

    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "job" or name.startswith("job.")
    }
    sys.modules["job"] = fake_job
    sys.modules["job.entities"] = fake_entities

    try:
        # Несмотря на отравленный кэш, наш загрузчик должен всё ещё работать
        settings_module = load_module("settings")
        settings = settings_module.load_dataset_settings(ENTITY_DIR / "config.yaml")
        # Этот вызов должен вернуть DatasetSettings из нашей энтити, не из подделки.
        # Сверяемся с самим config.yaml, а не с литералом: sample_percent —
        # настраиваемый параметр, и тест не должен падать при его изменении.
        raw = yaml.safe_load((ENTITY_DIR / "config.yaml").read_text(encoding="utf-8"))
        assert settings.sample_percent == int(raw["dataset"]["sample_percent"])
    finally:
        # Уборка
        for name in list(sys.modules.keys()):
            if name == "job" or name.startswith("job."):
                del sys.modules[name]
        sys.modules.update(saved)
