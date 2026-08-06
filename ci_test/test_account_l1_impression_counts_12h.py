import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENTITY_ROOT = (
    ROOT
    / "layers/silver/calculated_at_account_id_l1_category_id"
    / "account_l1_impression_counts_12h/v1"
)


def _load_job_module(module_name):
    module_path = ENTITY_ROOT / f"job/{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"account_l1_impression_counts_12h_{module_name}",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("2026-08-05T07:00:00Z", datetime(2026, 8, 5, 7, tzinfo=timezone.utc)),
        (
            "2026-08-05T07:00:00+00:00",
            datetime(2026, 8, 5, 7, tzinfo=timezone.utc),
        ),
        ("2026-08-05 07:00:00", datetime(2026, 8, 5, 7, tzinfo=timezone.utc)),
        (
            "2026-08-05T12:00:00+05:00",
            datetime(2026, 8, 5, 7, tzinfo=timezone.utc),
        ),
        (
            "2026-08-06T00:00:00+05:00",
            datetime(2026, 8, 5, 19, tzinfo=timezone.utc),
        ),
    ),
)
def test_partition_end_is_normalized_to_utc(value, expected):
    partition = _load_job_module("partition")

    assert partition.parse_airflow_timestamp(value) == expected


@pytest.mark.parametrize("value", ("", "2026/08/05", "not-a-timestamp", None))
def test_partition_end_rejects_unsupported_values(value):
    partition = _load_job_module("partition")

    with pytest.raises(ValueError, match=repr(value)):
        partition.parse_airflow_timestamp(value)


def test_source_config_drives_tables_window_and_category_depth():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    calculation_sql = query.build_account_l1_impression_counts_query(
        settings,
        datetime(2026, 8, 5, 7, tzinfo=timezone.utc),
    )
    depth_sql = query.build_category_depth_validation_query(settings)

    for table_name in settings.table_names:
        assert table_name in calculation_sql
    assert "event.event_type = 'PRODUCT_IMPRESSION'" in calculation_sql
    assert "c9.parent_id IS NOT NULL" in depth_sql
    assert " c10 " not in depth_sql
    assert settings.window_hours == 12


def test_query_uses_half_open_12_hour_window_and_session_distinct():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    sql = query.build_account_l1_impression_counts_query(
        settings,
        datetime(2026, 8, 5, 7, tzinfo=timezone.utc),
    )

    assert (
        "event.received_at >= "
        "TIMESTAMP '2026-08-04 19:00:00'"
    ) in sql
    assert (
        "event.received_at < "
        "TIMESTAMP '2026-08-05 07:00:00'"
    ) in sql
    assert "COUNT(DISTINCT product_id)" in sql
    assert "account_id,\n        session_id,\n        l1_category_id" in sql
    assert "SUM(n_impressions)" in sql
    assert "space" not in sql.lower()


def test_query_prefers_valid_event_category_then_product_fallback():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    sql = query.build_account_l1_impression_counts_query(
        settings,
        datetime(2026, 8, 5, 19, tzinfo=timezone.utc),
    )

    assert (
        "COALESCE(\n"
        "            event_category.l1_category_id,\n"
        "            product_category.l1_category_id\n"
        "        ) AS l1_category_id"
    ) in sql
    assert "value <> 1" in sql
    assert "WHERE l1_category_id IS NOT NULL" in sql


def test_contract_contains_schedule_schema_and_idempotent_partition():
    config = (ENTITY_ROOT / "config.yaml").read_text(encoding="utf-8")
    dag = (ENTITY_ROOT / "dag.py").read_text(encoding="utf-8")
    migration = (
        ENTITY_ROOT / "migrations/create_table.sql"
    ).read_text(encoding="utf-8")
    readme = (ENTITY_ROOT / "README.md").read_text(encoding="utf-8")

    assert (
        "primary_key: calculated_at,account_id,l1_category_id"
        in config
    )
    assert "resource_profile: large" in config
    assert "group_tag: recsys-main-page-features" in config
    assert 'schedule: "0 7,19 * * *"' in config
    assert 'start_date: "2026-06-01T00:00:00Z"' in config
    assert "catchup: false" in config
    assert 'catchup=dag_settings["catchup"]' in dag
    assert 'CronDataIntervalTimetable(dag_settings["schedule"], "UTC")' in dag
    assert "PARTITIONED BY (hours(calculated_at))" in migration
    assert "'engine.hive.lock-enabled' = 'false'" in migration
    assert "sku_id" not in migration
    assert "sku_group_id" not in migration
    assert "отдельный контролируемый backfill" in readme
    assert "Airflow group tag: `recsys-main-page-features`" in readme
