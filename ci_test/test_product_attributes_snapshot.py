import importlib.util
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
    assert 'start_date: "2026-08-04T19:00:00Z"' in config
    assert "catchup: false" in config
    assert 'catchup=dag_settings["catchup"]' in dag
    assert 'CronDataIntervalTimetable(dag_settings["schedule"], "UTC")' in dag
    assert "iceberg.silver_apidb_kazanexpress.public_category" in job
    assert "iceberg.silver.recsys_category_genders" in job
    assert "_validate_category_depth(spark)" in job
    assert "c9.parent_id <> {TECHNICAL_CATEGORY_ROOT_ID}" in job
    assert "NULLIF(" in job
    assert "CAST(product.category_id AS BIGINT)" in job
    assert "brand_name_id <> {EXCLUDED_BRAND_ID}" in job
    assert "MIN(brand_name_id)" in job
    assert "PARTITIONED BY (snapshot_date)" in migration
    assert "'engine.hive.lock-enabled' = 'false'" in migration
