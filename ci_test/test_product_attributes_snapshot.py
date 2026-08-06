import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENTITY_ROOT = (
    ROOT
    / "layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1"
)


def _load_partition_module():
    module_path = ENTITY_ROOT / "job/partition.py"
    spec = importlib.util.spec_from_file_location(
        "product_attributes_snapshot_partition",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_job_module(module_name):
    module_path = ENTITY_ROOT / f"job/{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"product_attributes_snapshot_{module_name}",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("2026-08-04T19:00:00Z", date(2026, 8, 5)),
        ("2026-08-04T19:00:00+00:00", date(2026, 8, 5)),
        ("2026-08-04T19:00:00", date(2026, 8, 5)),
        ("2026-08-04 19:00:00", date(2026, 8, 5)),
        ("2026-08-04 19:00:00+00:00", date(2026, 8, 5)),
        ("2026-08-05T00:00:00+05:00", date(2026, 8, 5)),
    ),
)
def test_snapshot_date_uses_tashkent_partition_end(value, expected):
    partition = _load_partition_module()

    assert partition.snapshot_date_from_partition_end(value) == expected


@pytest.mark.parametrize("value", ("", "2026/08/04", "not-a-timestamp", None))
def test_snapshot_date_rejects_unsupported_values(value):
    partition = _load_partition_module()

    with pytest.raises(ValueError, match=repr(value)):
        partition.snapshot_date_from_partition_end(value)


def test_source_config_drives_query_and_depth_validation():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    snapshot_sql = query.build_product_attributes_snapshot_query(
        settings,
        "2026-08-05",
    )
    depth_sql = query.build_category_depth_validation_query(settings)
    l6_validation_sql = query.build_required_l6_validation_query(settings)

    for table_name in settings.table_names:
        assert table_name in snapshot_sql
    assert settings.category_table in depth_sql
    assert settings.product_table in l6_validation_sql
    assert "product.category_id IS NULL" in l6_validation_sql
    assert "category.id IS NULL" in l6_validation_sql
    assert "category.id = 1" in l6_validation_sql
    assert "c9.parent_id IS NOT NULL" in depth_sql
    assert " c10 " not in depth_sql
    assert "brand_name_id <> 160078" in snapshot_sql
    assert "MIN(brand_name_id)" in snapshot_sql
    assert "dominant_gender IN ('M', 'F', 'U')" in snapshot_sql


def test_l6_is_last_non_technical_category_and_has_no_null_fallback():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    snapshot_sql = query.build_product_attributes_snapshot_query(
        settings,
        "2026-08-05",
    )

    assert "value <> 1" in snapshot_sql
    assert (
        "CAST(TRY_ELEMENT_AT(hierarchy, -1) AS BIGINT) "
        "AS l6_category_id"
    ) in snapshot_sql
    assert "hierarchy.l6_category_id" in snapshot_sql
    assert "NULLIF(" not in snapshot_sql


def test_query_rejects_invalid_snapshot_date():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    with pytest.raises(ValueError):
        query.build_product_attributes_snapshot_query(
            settings,
            "2026/08/05",
        )


def test_contract_contains_approved_runtime_and_business_rules():
    config = (ENTITY_ROOT / "config.yaml").read_text(encoding="utf-8")
    dag = (ENTITY_ROOT / "dag.py").read_text(encoding="utf-8")
    job = (
        ENTITY_ROOT / "job/getting_product_attributes_snapshot.py"
    ).read_text(encoding="utf-8")
    migration = (
        ENTITY_ROOT / "migrations/create_table.sql"
    ).read_text(encoding="utf-8")

    assert "primary_key: snapshot_date,product_id" in config
    assert "resource_profile: medium" in config
    assert 'schedule: "0 19 * * *"' in config
    assert 'start_date: "2026-06-01T00:00:00Z"' in config
    assert "catchup: false" in config
    assert 'catchup=dag_settings["catchup"]' in dag
    assert 'CronDataIntervalTimetable(dag_settings["schedule"], "UTC")' in dag
    assert "load_source_settings()" in job
    assert "_validate_category_depth(spark, settings)" in job
    assert "_validate_required_l6(spark, settings)" in job
    assert "PARTITIONED BY (snapshot_date)" in migration
    assert "Последняя содержательная категория пути" in migration
    assert "'engine.hive.lock-enabled' = 'false'" in migration
