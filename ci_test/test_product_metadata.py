import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENTITY_ROOT = ROOT / "layers/silver/product_id/product_metadata/v1"


def _load_partition_module():
    module_path = ENTITY_ROOT / "job/partition.py"
    spec = importlib.util.spec_from_file_location(
        "product_metadata_partition",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_job_module(module_name):
    module_path = ENTITY_ROOT / f"job/{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"product_metadata_{module_name}",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
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
def test_dt_uses_tashkent_partition_end(value, expected):
    partition = _load_partition_module()

    assert partition.dt_from_partition_end(value) == expected


@pytest.mark.parametrize("value", ("", "2026/08/04", "not-a-timestamp", None))
def test_dt_rejects_unsupported_values(value):
    partition = _load_partition_module()

    with pytest.raises(ValueError, match=repr(value)):
        partition.dt_from_partition_end(value)


def test_source_config_drives_fixed_six_level_query():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    metadata_sql = query.build_product_metadata_query(settings, "2026-08-05")
    depth_sql = query.build_category_depth_validation_query(settings)
    l6_validation_sql = query.build_required_l6_validation_query(settings)

    assert settings.max_category_depth == 6
    for table_name in settings.table_names:
        assert table_name in metadata_sql
    assert settings.category_table in depth_sql
    assert settings.product_table in l6_validation_sql
    assert "product.category_id IS NULL" in l6_validation_sql
    assert "category.id IS NULL" in l6_validation_sql
    assert "category.id = 1" in l6_validation_sql
    assert "c5.parent_id IS NOT NULL" in depth_sql
    assert " c6 " not in depth_sql
    assert "ARRAY(c0.id, c1.id, c2.id, c3.id, c4.id, c5.id)" in metadata_sql
    assert "brand_name_id != 160078" in metadata_sql
    assert "MIN(brand_name_id)" in metadata_sql
    assert "dominant_gender IN ('M', 'F', 'U')" in metadata_sql


def test_metadata_query_uses_int_ids_leaf_category_and_bang_equal():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    metadata_sql = query.build_product_metadata_query(settings, "2026-08-05")

    assert "DATE '2026-08-05' AS dt" in metadata_sql
    assert "CAST(product.id AS INT) AS product_id" in metadata_sql
    assert "CAST(product.category_id AS INT) AS category_id" in metadata_sql
    assert "CAST(category_id AS INT) AS l6_category_id" in metadata_sql
    assert "hierarchy.l6_category_id" in metadata_sql
    assert "value != 1" in metadata_sql
    assert "BIGINT" not in metadata_sql
    assert "NULLIF(" not in metadata_sql


def test_merge_replaces_only_current_dt_inside_month_partition():
    query = _load_job_module("query")

    merge_sql = query.build_product_metadata_merge_query(
        "iceberg.silver.feature_platform_product_metadata",
        "2026-08-05",
    )

    assert "MERGE INTO iceberg.silver.feature_platform_product_metadata" in merge_sql
    assert "target.dt = source.dt" in merge_sql
    assert "target.product_id = source.product_id" in merge_sql
    assert "WHEN MATCHED THEN UPDATE" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    assert "WHEN NOT MATCHED BY SOURCE" in merge_sql
    assert "target.dt = DATE '2026-08-05'" in merge_sql


def test_query_rejects_invalid_dt():
    runtime_config = _load_job_module("runtime_config")
    query = _load_job_module("query")
    settings = runtime_config.load_source_settings(ENTITY_ROOT / "config.yaml")

    with pytest.raises(ValueError):
        query.build_product_metadata_query(settings, "2026/08/05")

    with pytest.raises(ValueError):
        query.build_product_metadata_merge_query(
            "iceberg.silver.feature_platform_product_metadata",
            "2026/08/05",
        )


def test_contract_contains_approved_runtime_storage_and_business_rules():
    config = (ENTITY_ROOT / "config.yaml").read_text(encoding="utf-8")
    dag = (ENTITY_ROOT / "dag.py").read_text(encoding="utf-8")
    factory = (ENTITY_ROOT / "config/factory.py").read_text(encoding="utf-8")
    job = (ENTITY_ROOT / "job/getting_product_metadata.py").read_text(
        encoding="utf-8"
    )
    migration = (ENTITY_ROOT / "migrations/create_table.sql").read_text(
        encoding="utf-8"
    )

    assert "key: product_metadata" in config
    assert "name: feature_platform_product_metadata" in config
    assert "primary_key: dt,product_id" in config
    assert "layers/silver/product_id/product_metadata/v1" in config
    assert "feature-platform.layers.silver.product_id.product_metadata" in config
    assert "resource_profile: small" in config
    assert "max_category_depth: 6" in config
    assert 'schedule: "0 19 * * *"' in config
    assert 'start_date: "2026-06-01T00:00:00Z"' in config
    assert "catchup: false" in config
    assert 'catchup=dag_settings["catchup"]' in dag
    assert 'CronDataIntervalTimetable(dag_settings["schedule"], "UTC")' in dag
    assert 'spark.conf.set("spark.sql.ansi.enabled", "true")' in job
    assert "build_product_metadata_merge_query" in job
    assert "overwritePartitions" not in job
    assert '"<driver_memory_overhead>"' in factory
    assert '"<executor_memory_overhead>"' in factory
    assert "PARTITIONED BY (months(dt))" in migration
    assert "category_id INT COMMENT" in migration
    assert "l6_category_id INT COMMENT" in migration
    assert "BIGINT" not in migration
    assert "'engine.hive.lock-enabled' = 'false'" in migration
