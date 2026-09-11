"""Проверить атомарную замену дерева в локальном Iceberg без сети."""

from copy import deepcopy
from datetime import timedelta
import json
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import IdentityTransform

from ci_test.test_demand_catalog_tree import DAY, ENTITY, NOW, source
from layers.silver.level_node_id.demand_catalog_tree.v1.job import preparation as prep, writer


@pytest.fixture
def env(tmp_path):
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    table = catalog.create_table(prep.target_ref(cfg, catalog.name), schema=prep.expected_schema())
    with table.update_spec() as update:
        update.add_field("date", IdentityTransform(), "date")
    yield cfg, catalog, table.schema().as_arrow()
    catalog.engine.dispose()


def data(schema):
    return prep.prepare_tree([source()], schema, capture_date=DAY, catalog_version="v1", source_snapshot_id=123,
                             expected_source_rows=2, source_manifest_id="source",
                             source_contract_version="current_catalog_tree_v1", ingested_at=NOW)[0]


def write(env, batch=None, **options):
    args = dict(capture_date=DAY, catalog_version="v1", source_snapshot_id=123, expected_nodes=7,
                verify_source=lambda: True)
    return writer.write_prepared(env[0], env[1], data(env[2]) if batch is None else batch, **(args | options))


def target(env):
    return env[1].load_table(prep.target_ref(env[0], env[1].name))


def replace(batch, name, values):
    field = batch.schema.field(name)
    return batch.set_column(batch.schema.get_field_index(name), field, pa.array(values, type=field.type))


def test_full_replace_receipt_retry_and_no_tags(env):
    for _ in range(2):
        receipt = write(env)
        assert json.loads(json.dumps(receipt)) == receipt
        assert receipt["status"] == "written" and receipt["rows_written"] == 7
        assert receipt["catalog_sku_snapshot_id"] == 123
    assert target(env).scan().to_arrow().sort_by(writer.SORT).equals(data(env[2]), check_metadata=False)
    assert set(target(env).refs()) == {"main"}


def test_full_replace_removes_old_dates_and_nodes(env):
    write(env)
    batch = replace(data(env[2]), "date", [DAY + timedelta(days=1)] * 7)
    batch = replace(batch, "node_id", ["market", "l1:1", "l2:1", "l3:1", "l4:1", "l5:1", "leaf:20"])
    write(env, batch, capture_date=DAY + timedelta(days=1))
    assert target(env).scan().to_arrow().sort_by(writer.SORT).equals(batch, check_metadata=False)


@pytest.mark.parametrize("name,values", [
    ("node_id", ["market", "l1:1", "l2:1", "l3:1", "l4:1", "l5:1", "l5:1"]),
    ("node_id", ["market", "l1:01", "l2:1", "l3:1", "l4:1", "l5:1", "leaf:10"]),
    ("parent_id", [None, "market", "l1:1", "l2:1", "l3:1", "l4:1", "l4:1"]),
    ("parent_id", [None, "market", "l1:1", "l2:1", "l3:1", "l4:1", None]),
    ("parent_id", ["market", "market", "l1:1", "l2:1", "l3:1", "l4:1", "l5:1"]),
    ("level_code", [0, 1, 2, 3, 4, 5, 7]),
    ("is_passthrough", [False] * 7),
    ("catalog_sku_snapshot_id", [456] * 7),
    ("catalog_sku_snapshot_id", [123] * 6 + [456]),
    ("catalog_version", ["wrong"] * 7),
    ("source_contract_version", ["wrong"] * 7),
    ("source_manifest_id", [" "] * 7),
    ("source_manifest_id", ["a"] * 6 + ["b"]),
    ("date", [DAY + timedelta(days=1)] * 7),
    ("ingested_at", [None] * 7),
])
def test_bad_batch_preserves_previous_snapshot(env, name, values):
    write(env)
    before = target(env).current_snapshot().snapshot_id
    check = Mock(return_value=True)
    with pytest.raises(ValueError):
        write(env, replace(data(env[2]), name, values), verify_source=check)
    check.assert_not_called()
    assert target(env).current_snapshot().snapshot_id == before


def test_no_leaf_and_unattached_branch_are_rejected(env):
    batch = data(env[2])
    with pytest.raises(ValueError, match="полного пути"):
        write(env, batch.slice(0, 6), expected_nodes=6)
    extra = replace(batch.slice(1, 1), "node_id", ["l1:99"])
    with pytest.raises(ValueError, match="полного пути"):
        write(env, pa.concat_tables([batch, extra]), expected_nodes=8)


@pytest.mark.parametrize("options", [{"expected_nodes": 0}, {"expected_nodes": 6}, {"expected_nodes": True},
    {"source_snapshot_id": True}, {"capture_date": "2026-09-09"}, {"catalog_version": ""},
    {"verify_source": None}, {"expected_metadata_location": ""}])
def test_invalid_arguments_never_write(env, options):
    with pytest.raises(ValueError):
        write(env, **options)
    assert target(env).current_snapshot() is None


@pytest.mark.parametrize("value", [False, None, 1])
def test_failed_input_check_aborts_commit(env, value):
    write(env)
    before = target(env).current_snapshot().snapshot_id
    with pytest.raises(ValueError, match="Source snapshot"):
        write(env, verify_source=lambda: value)
    assert target(env).current_snapshot().snapshot_id == before


def test_source_exception_and_target_race(env):
    from pyiceberg.exceptions import CommitFailedException

    write(env)
    table = target(env)
    before = table.current_snapshot().snapshot_id
    with pytest.raises(RuntimeError, match="input expired"):
        write(env, verify_source=Mock(side_effect=RuntimeError("input expired")))
    assert target(env).current_snapshot().snapshot_id == before
    def race():
        table.append(data(env[2]))
        return True
    with pytest.raises(CommitFailedException):
        write(env, verify_source=race)
    with pytest.raises(ValueError, match="Целевая таблица изменилась"):
        write(env, expected_metadata_location="stale-target")


def test_readback_failure_can_retry_and_concurrent_readback_fails(env, monkeypatch):
    table = target(env)
    with monkeypatch.context() as patch:
        patch.setattr(env[1], "load_table", lambda _: table)
        patch.setattr(table, "scan", Mock(side_effect=RuntimeError("lost readback")))
        with pytest.raises(RuntimeError, match="lost readback"):
            write(env)
    assert write(env)["rows_written"] == 7
    table = target(env)
    scan = table.scan
    def raced_scan(**kwargs):
        table.append(data(env[2]))
        return scan(**kwargs)
    monkeypatch.setattr(env[1], "load_table", lambda _: table)
    monkeypatch.setattr(table, "scan", raced_scan)
    with pytest.raises(RuntimeError, match="изменился во время read-back"):
        write(env)


def test_readback_compares_content_not_only_count(env, monkeypatch):
    table = target(env)
    wrong = replace(data(env[2]), "catalog_version", ["wrong"] * 7)
    monkeypatch.setattr(env[1], "load_table", lambda _: table)
    monkeypatch.setattr(table, "scan", lambda **_: Mock(to_arrow=lambda: wrong))
    with pytest.raises(RuntimeError, match="не совпало"):
        write(env)


@pytest.mark.parametrize("key,value", [("schema", "silver.tree"), ("catalog", "other"),
    ("name", ""), ("name", "iceberg.silver.tree"), ("schema", None)])
def test_bad_identifier_before_io(env, key, value):
    cfg = deepcopy(env[0])
    cfg["table"][key] = value
    catalog = Mock()
    catalog.name = "iceberg"
    with pytest.raises(ValueError):
        writer.preflight(cfg, catalog)
    catalog.table_exists.assert_not_called()


def test_missing_migration_and_wrong_partition(env):
    cfg = deepcopy(env[0])
    cfg["table"]["name"] = "feature_platform_missing"
    with pytest.raises(ValueError, match="миграцию"):
        writer.preflight(cfg, env[1])
    assert not env[1].table_exists(prep.target_ref(cfg, env[1].name))
    table = target(env)
    with table.update_spec() as update:
        update.remove_field("date")
    with pytest.raises(ValueError, match="identity partition"):
        write(env)
