"""Проверить точный seller DQ/snapshot binding на локальном Iceberg."""

from copy import deepcopy
from datetime import timedelta
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog

from ci_test.test_demand_catalog_sku_preparation import NOW, ROOT, arguments, replace
from layers.silver.sku_id.demand_catalog_sku.v1.job import inputs, preparation as prep


@pytest.fixture
def env(tmp_path):
    cfg = yaml.safe_load((ROOT / "layers/silver/sku_id/demand_catalog_sku/v1/config.yaml").read_text())
    source, schema = inputs.source_config(cfg, ROOT)
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    table = catalog.create_table(prep.target_ref(source, catalog.name), schema=schema)
    batch = replace(arguments()["seller"], "source_contract_version", [source["source"]["contract_version"]] * 2)
    table.append(batch.cast(table.schema().as_arrow()))
    receipt = dict(status="written", rows_written=2, snapshot_id=table.current_snapshot().snapshot_id,
                   table_uuid=str(table.metadata.table_uuid), catalog_version="catalog1",
                   source_contract_version=source["source"]["contract_version"], source_manifest_id="sellers1",
                   date_min="2026-09-09", date_max="2026-09-09", ingested_at=NOW.isoformat())
    reference = dict(dag_id=source["dag"]["id"], run_id="seller-run")
    checked = dict(dq_status="passed", **reference, receipt=receipt)
    yield cfg, source, schema, catalog, table, reference, checked
    catalog.engine.dispose()


def bind(env, checked=None, **kwargs):
    return inputs.bind_source(env[1], env[5], env[6] if checked is None else checked,
                              captured_at=kwargs.get("captured_at", NOW))


def test_exact_local_snapshot_flows_to_preparation(env):
    from ci_test.test_demand_catalog_sku_preparation import raw_source
    bound = bind(env)
    table, schema = inputs.preflight_source(env[1], env[3], bound, env[2])
    args = arguments() | dict(seller=table.scan(snapshot_id=bound["receipt"]["snapshot_id"]).to_arrow(), bound_seller=bound)
    result, _ = prep.prepare_catalog(raw_source(), prep.expected_schema(), **args)
    assert result["catalog_seller_snapshot_id"].to_pylist() == [bound["receipt"]["snapshot_id"]] * 3
    assert schema == env[4].schema().as_arrow()
    bound["receipt"]["source_manifest_id"] = "mutated"
    assert env[6]["receipt"]["source_manifest_id"] == "sellers1"


def test_new_head_and_new_schema_do_not_replace_chosen_snapshot(env):
    from pyiceberg.types import StringType
    bound = bind(env)
    table = env[4]
    with table.update_schema() as update:
        update.add_column("future_field", StringType())
    old = table.scan(snapshot_id=bound["receipt"]["snapshot_id"]).to_arrow()
    batch = old.append_column("future_field", pa.array([None, None], pa.large_string()))
    table.overwrite(batch.cast(table.schema().as_arrow()))
    assert table.current_snapshot().snapshot_id != bound["receipt"]["snapshot_id"]
    _, old_schema = inputs.preflight_source(env[1], env[3], bound, env[2])
    assert len(old_schema) == 12 and "future_field" not in old_schema.names


@pytest.mark.parametrize("field,value", [("dq_status", "failed"), ("dq_status", "missing"),
    ("run_id", "different"), ("dag_id", "different"), ("receipt", None)])
def test_wrong_or_missing_exact_dq_rejected(env, field, value):
    checked = deepcopy(env[6])
    checked[field] = value
    with pytest.raises(ValueError):
        bind(env, checked)


@pytest.mark.parametrize("field,value", [("status", "success"), ("snapshot_id", 0), ("snapshot_id", True),
    ("rows_written", 0), ("source_manifest_id", ""), ("source_contract_version", "other"),
    ("catalog_version", None), ("table_uuid", "bad"), ("table_uuid", "00000000-0000-0000-0000-000000000000"),
    ("date_max", "2026-09-10"), ("date_min", "2026-09-08"), ("ingested_at", "2026-09-09T05:00:00"),
    ("ingested_at", "2026-09-10T05:00:00+00:00")])
def test_bad_receipts_rejected(env, field, value):
    checked = deepcopy(env[6])
    checked["receipt"][field] = value
    with pytest.raises(ValueError):
        bind(env, checked)


def test_old_capture_allowed_but_future_or_naive_materialization_rejected(env):
    assert bind(env, captured_at=NOW + timedelta(days=1))["captured_at"] == NOW
    for moment in (NOW - timedelta(seconds=1), NOW.replace(tzinfo=None)):
        with pytest.raises(ValueError):
            bind(env, captured_at=moment)


def test_missing_snapshot_uuid_or_migration_never_uses_latest(env):
    for field, value in (("snapshot_id", 1), ("table_uuid", "00000000-0000-0000-0000-000000000001")):
        bound = bind(env)
        bound["receipt"][field] = value
        with pytest.raises(ValueError, match="latest запрещён"):
            inputs.preflight_source(env[1], env[3], bound, env[2])
    catalog = Mock(name="catalog")
    catalog.name = "iceberg"
    catalog.table_exists.return_value = False
    with pytest.raises(ValueError, match="миграции"):
        inputs.preflight_source(env[1], catalog, bind(env), env[2])
    catalog.table_exists.assert_called_once_with((env[1]["table"]["schema"], env[1]["table"]["name"]))
    catalog.load_table.assert_not_called()


def test_wrong_expected_schema_and_missing_snapshot_schema_block(env):
    with pytest.raises(ValueError, match="Схема seller"):
        inputs.preflight_source(env[1], env[3], bind(env), pa.schema(list(env[2])[:-1]))
    catalog, table = Mock(), Mock()
    catalog.name = "iceberg"
    catalog.table_exists.return_value = True
    catalog.load_table.return_value = table
    table.metadata.table_uuid = env[6]["receipt"]["table_uuid"]
    table.snapshot_by_id.return_value.schema_id = 99
    table.schemas.return_value = {}
    with pytest.raises(ValueError, match="схема точного"):
        inputs.preflight_source(env[1], catalog, bind(env), env[2])


def test_reference_and_config_path_rejected_before_io(env, tmp_path):
    for reference in ({}, {"dag_id": env[5]["dag_id"], "run_id": " "}, {**env[5], "extra": True}):
        with pytest.raises(ValueError):
            inputs.validate_reference(env[1], reference)
    cfg = deepcopy(env[0])
    cfg["inputs"]["seller_config"] = str(tmp_path / "outside.yaml")
    with pytest.raises(ValueError, match="внутри FP"):
        inputs.source_config(cfg, ROOT)
