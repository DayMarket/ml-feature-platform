import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENTITY_ROOT = (
    ROOT
    / "layers/silver/calculated_at_account_id_session_id_product_id_event_type"
    / "account_product_session_action_counts_12h/v1"
)


def _load_job_module(module_name):
    module_path = ENTITY_ROOT / f"job/{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"account_product_session_action_counts_12h_{module_name}",
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


def test_source_config_drives_event_types_and_window():
    runtime_config = _load_job_module("runtime_config")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    assert settings.events_table == "iceberg.silver_b2c_clickstream.events"
    assert settings.event_types == (
        "PRODUCT_VIEW",
        "ADD_TO_CART",
        "ADD_TO_FAVORITES",
    )
    assert settings.window_hours == 12
    assert settings.table_names == (settings.events_table,)


def test_query_uses_exact_action_types_and_no_space_filter():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    sql = query.build_account_product_session_action_counts_query(
        settings,
        datetime(2026, 8, 5, 7, tzinfo=timezone.utc),
    )

    for event_type in settings.event_types:
        assert f"'{event_type}'" in sql
    assert "PRODUCT_IMPRESSION" not in sql
    assert "space" not in sql.lower()


def test_query_uses_half_open_window_and_positive_entity_filters():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    sql = query.build_account_product_session_action_counts_query(
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
    assert "CAST(event.account_id AS BIGINT) > 0" in sql
    assert "CAST(event.product_id AS BIGINT) > 0" in sql
    assert "event.session_id IS NOT NULL" in sql


def test_query_counts_raw_events_and_keeps_last_received_at():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    sql = query.build_account_product_session_action_counts_query(
        settings,
        datetime(2026, 8, 5, 19, tzinfo=timezone.utc),
    )

    assert "CAST(COUNT(*) AS BIGINT) AS n_events" in sql
    assert (
        "CAST(MAX(received_at) AS TIMESTAMP) AS last_received_at"
        in sql
    )
    assert (
        "account_id,\n"
        "    session_id,\n"
        "    product_id,\n"
        "    event_type"
    ) in sql


def test_contract_contains_schedule_schema_and_idempotent_partition():
    config = (ENTITY_ROOT / "config.yaml").read_text(encoding="utf-8")
    dag = (ENTITY_ROOT / "dag.py").read_text(encoding="utf-8")
    migration = (
        ENTITY_ROOT / "migrations/create_table.sql"
    ).read_text(encoding="utf-8")
    readme = (ENTITY_ROOT / "README.md").read_text(encoding="utf-8")

    assert (
        "primary_key: "
        "calculated_at,account_id,session_id,product_id,event_type"
    ) in config
    assert "resource_profile: large" in config
    assert "group_tag: recsys-main-page-features" in config
    assert 'schedule: "0 7,19 * * *"' in config
    assert 'start_date: "2026-07-05T19:00:00Z"' in config
    assert "catchup: false" in config
    assert 'catchup=dag_settings["catchup"]' in dag
    assert 'CronDataIntervalTimetable(dag_settings["schedule"], "UTC")' in dag
    assert "PARTITIONED BY (hours(calculated_at))" in migration
    assert "'engine.hive.lock-enabled' = 'false'" in migration
    assert "n_events BIGINT" in migration
    assert "last_received_at TIMESTAMP" in migration
    assert "category_id" not in migration
    assert "sku_id" not in migration
    assert "sku_group_id" not in migration
    assert "31" in readme
    assert "Airflow group tag: `recsys-main-page-features`" in readme
