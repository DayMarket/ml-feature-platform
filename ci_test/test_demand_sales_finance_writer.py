"""Атомарная запись sales/finance дня и composite-key resume в локальном Iceberg."""

from datetime import date
import json

import pyarrow as pa
import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, config, fx, module, raw, schema


@pytest.fixture(params=["sales", "finance"])
def env(request, tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog
    kind = request.param
    cat = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                     warehouse=(tmp_path / "warehouse").as_uri())
    cat.create_namespace("silver")
    cfg = config(kind)
    table = cat.create_table(module(kind, "preparation").target_ref(cfg, cat.name), schema=schema(kind))
    with table.update_spec() as spec:
        spec.add_identity("date")
    yield kind, cfg, cat, table
    cat.engine.dispose()


def prepared(env, *, day=DAY, changes=None):
    kind, _, _, table = env
    source = raw(kind, {"date": day} | (changes or {}))
    return module(kind, "preparation").prepare_batch(
        source, table.schema().as_arrow(), day=day,
        fx=fx() | {"date": day, "fx_rate_date": day},
        manifest="capture-1", version="v1", ingested_at=CAPTURE)


def write(env, batches=None, *, day=DAY, expected=1, verify=lambda: True):
    kind, cfg, cat, _ = env
    return module(kind, "writer").write_day(
        cfg, cat, [prepared(env, day=day)] if batches is None else batches,
        day=day, expected_rows=expected, manifest="capture-1", version="v1",
        ingested_at=CAPTURE, verify_source=verify)


def resume(env, day=DAY):
    kind, cfg, cat, _ = env
    return module(kind, "checkpoint").resume_day(cfg, cat, day=day, manifest="capture-1", version="v1")


def test_atomic_day_replaces_keys_and_preserves_other_dates(env):
    _, _, _, table = env
    earlier = date(2026, 9, 5)
    write(env, day=earlier)
    write(env)
    write(env, [prepared(env, changes={"sku_id": 2})])
    table.refresh()
    rows = table.scan().to_arrow().to_pylist()
    assert {(r["date"], r["sku_id"]) for r in rows} == {(earlier, 1), (DAY, 2)}
    assert set(table.refs()) == {"main"}


@pytest.mark.parametrize("failure", ["partial", "source", "stream", "duplicate"])
def test_no_half_day_commit(env, failure):
    _, _, _, table = env
    old = write(env)
    data = prepared(env, changes={"sku_id": 2})
    def stream():
        yield data
        raise RuntimeError("source interrupted")
    batches = [] if failure == "partial" else stream() if failure == "stream" else [data]
    if failure == "duplicate":
        batches += [data]
    with pytest.raises((ValueError, RuntimeError)):
        write(env, batches, expected=2 if failure == "duplicate" else 1,
              verify=lambda: failure != "source")
    table.refresh()
    assert table.current_snapshot().snapshot_id == old["snapshot_id"]
    assert table.scan().to_arrow()["sku_id"].to_pylist() == [1]


def test_resume_rechecks_written_without_new_commit(env):
    kind, _, _, table = env
    old = write(env)
    proof = table.refresh().current_snapshot().summary.additional_properties[
        module(kind, "checkpoint").PROOF_KEY]
    assert json.loads(proof)["rows_written"] == 1
    receipt = resume(env)
    assert receipt["resumed"] and receipt["status"] == "written"
    assert receipt["snapshot_id"] == old["snapshot_id"]
    assert "dq_status" not in receipt


def test_changed_contents_same_keys_count_not_reused(env):
    from pyiceberg.expressions import EqualTo
    _, _, _, table = env
    write(env)
    changed = prepared(env)
    field = "sales_payment_value" if env[0] == "sales" else "finance_gmv_generated"
    from decimal import Decimal
    changed = changed.set_column(changed.schema.get_field_index(field), changed.schema.field(field),
                                 pa.array([Decimal(90)], type=changed.schema.field(field).type))
    table.refresh()
    table.overwrite(changed, overwrite_filter=EqualTo("date", DAY))
    assert resume(env) is None


def test_successful_commit_recovers_after_worker_lost_ack(env, monkeypatch):
    def lost(*args):
        raise RuntimeError("lost acknowledgement")
    monkeypatch.setattr(module(env[0], "writer"), "verify_proof", lost)
    with pytest.raises(RuntimeError, match="acknowledgement"):
        write(env)
    assert resume(env)["resumed"]


def test_finance_same_sku_different_seller_cross_batch_checkpoint(env):
    if env[0] != "finance":
        pytest.skip("Только composite-key finance")
    first = prepared(env, changes={"seller_id": 10, "seller_key": "seller:10"})
    second = prepared(env, changes={"seller_id": 2, "seller_key": "seller:2"})
    third = prepared(env, changes={"seller_id": None, "seller_key": "unknown"})
    write(env, [first, second, third], expected=3)
    assert resume(env)["rows_written"] == 3
    with pytest.raises(ValueError, match="порядок SKU"):
        write(env, [second, first], expected=2)
    assert resume(env)["rows_written"] == 3
