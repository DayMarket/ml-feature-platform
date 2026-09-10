"""Проверить полный SKU commit, read-back, идемпотентность и отказ до публикации snapshot."""

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

from ci_test.test_demand_catalog_sku_preparation import NOW, ROOT, arguments, raw_source
from layers.silver.sku_id.demand_catalog_sku.v1.job import preparation as prep, writer


@pytest.fixture
def env(tmp_path):
    cfg = yaml.safe_load((ROOT / "layers/silver/sku_id/demand_catalog_sku/v1/config.yaml").read_text())
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    table = catalog.create_table(prep.target_ref(cfg, catalog.name), schema=prep.expected_schema())
    with table.update_spec() as update:
        update.add_field("date", IdentityTransform(), "date")
    yield cfg, catalog
    catalog.engine.dispose()


def write(env, source=None, **changes):
    args = arguments()
    args.pop("source_contract_version")
    return writer.write_catalog(*env, raw_source() if source is None else source,
                                 **(args | dict(verify_source=lambda: True) | changes))


def target(env):
    return env[1].load_table(prep.target_ref(env[0], env[1].name))


def test_receipt_exact_snapshot_full_dq_binding_and_retry(env):
    for _ in range(2):
        receipt = write(env)
        assert json.loads(json.dumps(receipt)) == receipt
        assert receipt["status"] == "written" and receipt["rows_written"] == 3
        assert receipt["catalog_seller_snapshot_id"] == 123
    table = target(env)
    expected, _ = prep.prepare_catalog(raw_source(), table.schema().as_arrow(), **arguments())
    assert table.scan().to_arrow().sort_by([("sku_id", "ascending")]).equals(expected, check_metadata=False)
    assert set(table.refs()) == {"main"}


def test_full_replace_removes_disappeared_sku_and_old_date(env):
    write(env)
    counts = arguments()["counts"] | {"sku": 1}
    receipt = write(env, raw_source().slice(0, 1), counts=counts, ingested_at=NOW + timedelta(days=1))
    batch = target(env).scan().to_arrow()
    assert receipt["rows_written"] == 1 and batch["sku_id"].to_pylist() == [1]
    assert batch["date"][0].as_py() == (NOW + timedelta(days=1)).date()


@pytest.mark.parametrize("value", [None, False, 1, "passed"])
def test_failed_recheck_does_not_commit_or_remove_previous_catalog(env, value):
    first = write(env)
    with pytest.raises(ValueError, match="перед commit"):
        write(env, verify_source=lambda: value)
    assert target(env).current_snapshot().snapshot_id == first["snapshot_id"]
    assert target(env).scan().to_arrow().num_rows == 3


def test_callback_exception_and_missing_callback(env):
    first = write(env)
    def changed():
        raise RuntimeError("source changed")
    with pytest.raises(RuntimeError, match="source changed"):
        write(env, verify_source=changed)
    with pytest.raises(ValueError, match="повторная проверка"):
        write(env, verify_source=None)
    assert target(env).current_snapshot().snapshot_id == first["snapshot_id"]


def test_source_failure_and_wrong_metadata_leave_previous_snapshot(env):
    first = write(env)
    args = arguments()
    args["active_links"][0]["meta_present"] = 0
    with pytest.raises(ValueError, match="source readiness"):
        write(env, active_links=args["active_links"])
    with pytest.raises(ValueError, match="target изменился"):
        write(env, expected_metadata_location="previous-metadata")
    assert target(env).current_snapshot().snapshot_id == first["snapshot_id"]


def test_target_contract_and_identifiers_checked_before_source(env):
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.table_exists.return_value = False
    with pytest.raises(ValueError, match="миграции"):
        writer.preflight(env[0], catalog)
    catalog.table_exists.assert_called_once_with((env[0]["table"]["schema"], env[0]["table"]["name"]))
    catalog.load_table.assert_not_called()
    for field, value in (("catalog", "other"), ("name", "silver.table"), ("schema", "")):
        config = deepcopy(env[0])
        config["table"][field] = value
        with pytest.raises(ValueError):
            writer.preflight(config, env[1])
    wrong = env[1].create_table(("silver", "wrong"), schema=prep.expected_schema())
    config = deepcopy(env[0])
    config["table"]["name"] = "wrong"
    with pytest.raises(ValueError, match="identity partition"):
        writer.preflight(config, env[1])
    with wrong.update_spec() as update:
        update.add_field("date", IdentityTransform(), "date")
    with wrong.update_schema() as update:
        update.delete_column("unit_id")
    with pytest.raises(ValueError, match="38 согласованных"):
        writer.preflight(config, env[1])


def test_required_nullable_contract(env):
    schema = prep.expected_schema()
    index = schema.get_field_index("sku_id")
    with pytest.raises(ValueError, match="nullable"):
        prep.validate_schema(schema.set(index, pa.field("sku_id", pa.int64(), nullable=True)))


def test_concurrent_writer_wins_without_being_overwritten(env):
    from pyiceberg.exceptions import CommitFailedException
    write(env)
    committed = []
    def competing_write():
        committed.append(write(env, raw_source().slice(0, 1), counts=arguments()["counts"] | {"sku": 1}))
        return True
    with pytest.raises(CommitFailedException):
        write(env, verify_source=competing_write)
    assert target(env).current_snapshot().snapshot_id == committed[0]["snapshot_id"]
    assert target(env).scan().to_arrow().num_rows == 1


def test_failed_readback_does_not_return_written_receipt(env, monkeypatch):
    table = target(env)
    real_scan = table.scan
    def partial_readback(**kwargs):
        scan = Mock()
        scan.to_arrow.return_value = real_scan(**kwargs).to_arrow().slice(0, 1)
        return scan
    monkeypatch.setattr(table, "scan", partial_readback)
    monkeypatch.setattr(writer, "preflight", lambda *args: table)
    with pytest.raises(RuntimeError, match="read-back"):
        write(env)
    # Commit уже состоялся; ошибка read-back не означает отката таблицы.
    assert target(env).scan().to_arrow().num_rows == 3


def test_concurrent_change_during_readback_prevents_success_receipt(env, monkeypatch):
    table = target(env)
    real_scan = table.scan
    competing = []
    def readback_with_new_head(**kwargs):
        actual = real_scan(**kwargs).to_arrow()
        other = target(env)
        other.overwrite(actual.slice(0, 1))
        competing.append(other.current_snapshot().snapshot_id)
        result = Mock()
        result.to_arrow.return_value = actual
        return result
    monkeypatch.setattr(table, "scan", readback_with_new_head)
    monkeypatch.setattr(writer, "preflight", lambda *args: table)
    with pytest.raises(RuntimeError, match="snapshot изменился во время read-back"):
        write(env)
    assert competing and target(env).current_snapshot().snapshot_id == competing[0]
    assert target(env).scan().to_arrow().num_rows == 1
