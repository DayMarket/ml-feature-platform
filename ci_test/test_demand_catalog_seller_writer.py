"""Проверить full-refresh seller-каталога на локальном Iceberg без сети."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import IdentityTransform

from layers.silver.seller_id.demand_catalog_seller.v1.job import preparation as prep, writer

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/seller_id/demand_catalog_seller/v1"
CAPTURE = datetime(2026, 9, 8, 21, 0, 0, 123456, tzinfo=timezone.utc)


@pytest.fixture
def env(tmp_path):
    config = yaml.safe_load((ENTITY / "config.yaml").read_text())
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    table = catalog.create_table(prep.target_ref(config, catalog.name), schema=prep.expected_schema())
    with table.update_spec() as update:
        update.add_field("date", IdentityTransform(), "date")
    yield config, catalog, table.schema().as_arrow()
    catalog.engine.dispose()


def data(schema, *, ids=(1, 2, 3), masters=("master-a", "", None), capture=CAPTURE):
    raw = pa.table({"seller_id": pa.array(ids, type=pa.int64()),
                    "source_master_seller_id": pa.array(masters, type=pa.string()),
                    "is_1p": pa.array([None] * len(ids), type=pa.bool_()),
                    "seller_registered_at": pa.array([CAPTURE] * len(ids), type=pa.timestamp("us", "UTC"))})
    return prep.prepare_catalog(raw, schema, expected_source_rows=len(ids), catalog_version="catalog-1",
                                source_manifest_id="capture-1", source_contract_version="current_seller_catalog_v1",
                                ingested_at=capture)


def write(env, batch=None, *, verify_source=lambda: True):
    cfg, catalog, schema = env
    batch = data(schema) if batch is None else batch
    return writer.write_prepared(cfg, catalog, batch, expected_source_rows=batch.num_rows,
                                 verify_source=verify_source)


def table_for(env):
    cfg, catalog, _ = env
    return catalog.load_table(prep.target_ref(cfg, catalog.name))


def replace(batch, name, values):
    field = batch.schema.field(name)
    return batch.set_column(batch.schema.get_field_index(name), field, pa.array(values, type=field.type))


def test_full_replace_retry_receipt_preserves_unknown_and_has_no_tags(env):
    for _ in range(2):
        receipt = write(env)
        assert json.loads(json.dumps(receipt)) == receipt
        assert receipt["status"] == "written"
        assert receipt["rows_written"] == 3
        assert receipt["date_min"] == receipt["date_max"] == "2026-09-09"
        assert receipt["ingested_at"] == "2026-09-08T21:00:00.123456+00:00"
        assert receipt["catalog_version"] == "catalog-1"
    actual = table_for(env).scan().to_arrow().sort_by([("seller_id", "ascending")])
    assert actual.equals(data(env[2]), check_metadata=False)
    assert actual["seller_mapping_status"].to_pylist() == ["matched", "unmatched", "unavailable"]
    assert actual["is_1p"].null_count == 3
    assert set(table_for(env).refs()) == {"main"}


def test_full_replace_removes_old_dates_and_absent_sellers(env):
    write(env)
    replacement = data(env[2], ids=(2,), masters=("new-master",), capture=CAPTURE.replace(day=9))
    write(env, replacement)
    assert table_for(env).scan().to_arrow().equals(replacement, check_metadata=False)


@pytest.mark.parametrize("kind", ["empty", "duplicate", "null_key", "mixed_capture", "mixed_version",
    "mixed_manifest", "blank_manifest", "wrong_date", "fake_master", "fake_status", "fake_has_master",
    "wrong_contract", "null_required", "missing_field", "collision"])
def test_invalid_batch_does_not_change_previous_snapshot(env, kind):
    write(env)
    before = table_for(env).current_snapshot().snapshot_id
    batch = data(env[2])
    if kind == "empty":
        batch = batch.slice(0, 0)
    elif kind == "duplicate":
        batch = pa.concat_tables([batch, batch])
    elif kind == "missing_field":
        batch = batch.drop(["has_master"])
    else:
        name, values = {
            "null_key": ("seller_id", [None, 2, 3]),
            "mixed_capture": ("ingested_at", [CAPTURE.replace(tzinfo=None)] * 2 + [CAPTURE.replace(day=9, tzinfo=None)]),
            "mixed_version": ("catalog_version", ["a", "b", "a"]),
            "mixed_manifest": ("source_manifest_id", ["a", "b", "a"]),
            "blank_manifest": ("source_manifest_id", [" "] * 3),
            "wrong_date": ("date", [CAPTURE.date()] * 3),
            "fake_master": ("master_seller_id", ["other", "2", None]),
            "fake_status": ("seller_mapping_status", ["unmatched", "unmatched", "unavailable"]),
            "fake_has_master": ("has_master", [False, False, None]),
            "wrong_contract": ("source_contract_version", ["old-contract"] * 3),
            "null_required": ("catalog_version", [None] * 3),
            "collision": ("source_master_seller_id", ["2", "", None]),
        }[kind]
        batch = replace(batch, name, values)
    check = Mock(return_value=True)
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        write(env, batch, verify_source=check)
    check.assert_not_called()
    assert table_for(env).current_snapshot().snapshot_id == before


@pytest.mark.parametrize("result", [False, None, 1])
def test_failed_source_check_aborts_transaction(env, result):
    write(env)
    before = table_for(env).current_snapshot().snapshot_id
    check = Mock(return_value=result)
    with pytest.raises(ValueError, match="Source capture"):
        write(env, data(env[2], ids=(2,), masters=("",)), verify_source=check)
    check.assert_called_once_with()
    assert table_for(env).current_snapshot().snapshot_id == before
    assert table_for(env).scan().to_arrow().num_rows == 3


def test_source_exception_aborts_and_missing_callback_is_rejected(env):
    write(env)
    before = table_for(env).current_snapshot().snapshot_id
    with pytest.raises(RuntimeError, match="source unavailable"):
        write(env, verify_source=Mock(side_effect=RuntimeError("source unavailable")))
    with pytest.raises(ValueError, match="повторная проверка"):
        write(env, verify_source=None)
    assert table_for(env).current_snapshot().snapshot_id == before


def test_concurrent_commit_before_commit_is_not_overwritten(env):
    from pyiceberg.exceptions import CommitFailedException

    write(env)

    def changed_source_check():
        table_for(env).append(data(env[2], ids=(4,), masters=("another",)))
        return True

    with pytest.raises(CommitFailedException):
        write(env, verify_source=changed_source_check)
    assert set(table_for(env).scan().to_arrow()["seller_id"].to_pylist()) == {1, 2, 3, 4}


@pytest.mark.parametrize("expected", [None, True, 0, 2, 4])
def test_source_count_cannot_be_omitted_or_disagree(env, expected):
    with pytest.raises(ValueError, match="source count"):
        writer.write_prepared(env[0], env[1], data(env[2]), expected_source_rows=expected,
                              verify_source=lambda: True)
    assert table_for(env).current_snapshot() is None


@pytest.mark.parametrize("part,value", [("catalog", "other"), ("schema", "silver.table"),
    ("name", "silver.table"), ("name", ""), ("schema", None)])
def test_identifier_rejected_before_catalog_access(env, part, value):
    cfg = deepcopy(env[0])
    cfg["table"][part] = value
    catalog = Mock(name="catalog")
    catalog.name = "iceberg"
    with pytest.raises(ValueError):
        writer.preflight(cfg, catalog)
    catalog.table_exists.assert_not_called()


def test_missing_table_is_not_created(env):
    cfg = deepcopy(env[0])
    cfg["table"]["name"] = "feature_platform_missing"
    with pytest.raises(ValueError, match="миграцию"):
        writer.preflight(cfg, env[1])
    assert not env[1].table_exists(("silver", "feature_platform_missing"))


def test_wrong_partition_fails_preflight(env):
    table = table_for(env)
    with table.update_spec() as update:
        update.remove_field("date")
    with pytest.raises(ValueError, match="identity partition"):
        write(env)
    assert table_for(env).current_snapshot() is None


def test_readback_mismatch_never_returns_written(env, monkeypatch):
    table = table_for(env)
    monkeypatch.setattr(table, "scan", lambda **_: Mock(to_arrow=lambda: data(env[2]).slice(0, 1)))
    monkeypatch.setattr(env[1], "load_table", lambda _: table)
    with pytest.raises(RuntimeError, match="не совпал"):
        write(env)


def test_concurrent_commit_during_readback_is_not_accepted(env, monkeypatch):
    table = table_for(env)
    original = table.scan

    def scan(**kwargs):
        table.append(data(env[2], ids=(4,), masters=("another",)))
        return original(**kwargs)

    monkeypatch.setattr(table, "scan", scan)
    monkeypatch.setattr(env[1], "load_table", lambda _: table)
    with pytest.raises(RuntimeError, match="изменился во время read-back"):
        write(env)


def test_retry_after_readback_failure_replaces_complete_capture(env, monkeypatch):
    table = table_for(env)
    with monkeypatch.context() as patch:
        patch.setattr(env[1], "load_table", lambda _: table)
        patch.setattr(table, "scan", Mock(side_effect=RuntimeError("lost readback")))
        with pytest.raises(RuntimeError, match="lost readback"):
            write(env)
    assert table_for(env).scan().to_arrow().num_rows == 3
    assert write(env)["status"] == "written"
    assert table_for(env).scan().to_arrow().num_rows == 3
