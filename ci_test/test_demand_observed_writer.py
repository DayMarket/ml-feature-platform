"""Gold observed: настоящий локальный Iceberg, атомарность и точный read-back."""

from copy import deepcopy
from datetime import date
from decimal import Decimal
from importlib import import_module
import json

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_observed_preparation import DAY, ENTITY, NOW, PREP, VERSIONS, schema, source

WRITER = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.writer")
CHECKPOINT = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.checkpoint")


@pytest.fixture
def env(tmp_path):
    SqlCatalog = pytest.importorskip("pyiceberg.catalog.sql").SqlCatalog

    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("gold")
    config = yaml.safe_load((ENTITY / "config.yaml").read_text())
    table = catalog.create_table(PREP.target_ref(config, catalog.name), schema=schema(ENTITY))
    with table.update_spec() as spec:
        spec.add_identity("date")
    yield config, catalog, table
    catalog.engine.dispose()


def prepared(env, *, day=DAY, sales=(1, 3), stock=(2, 3), inputs=None):
    return list(PREP.join_batches(
        [source("sales", sales, date=day, sales_gmv=Decimal("12345678901234567890123456789012345678"))],
        [source("stock", stock, date=day)], env[2].schema().as_arrow(),
        day=day, inputs=inputs or VERSIONS, manifest="gold-join",
        version=env[0]["source"]["contract_version"],
        ingested_at=NOW, max_batch_rows=2))


def write(env, batches=None, *, day=DAY, expected=3, inputs=None, verify=lambda: True):
    return WRITER.write_day(
        env[0], env[1], prepared(env, day=day) if batches is None else batches,
        day=day, expected_rows=expected, inputs=inputs or VERSIONS, manifest="gold-join",
        version=env[0]["source"]["contract_version"], ingested_at=NOW, verify_inputs=verify)


def resume(env, **changes):
    args = dict(day=DAY, inputs=VERSIONS, manifest="gold-join",
                version=env[0]["source"]["contract_version"])
    return CHECKPOINT.resume_day(env[0], env[1], **(args | changes))


def test_replaces_complete_day_preserving_neighbors_and_decimal(env):
    earlier = date(2026, 8, 31)
    write(env, day=earlier)
    write(env)
    result = write(env, prepared(env, sales=(4,), stock=(5,)), expected=2)
    table = env[2].refresh()
    rows = table.scan().to_arrow().to_pylist()
    assert {(r["date"], r["sku_id"]) for r in rows} == {
        (earlier, 1), (earlier, 2), (earlier, 3), (DAY, 4), (DAY, 5)}
    assert next(r for r in rows if r["sku_id"] == 4)["sales_gmv"] == Decimal("12345678901234567890123456789012345678")
    assert next(r for r in rows if r["sku_id"] == 5)["sales_gmv"] is None
    assert next(r for r in rows if r["sku_id"] == 5)["is_in_stock_eod"] is True
    assert result["status"] == "written" and "dq_status" not in result
    assert set(table.refs()) == {"main"}
    assert resume(env)["resumed"]


@pytest.mark.parametrize("failure", ["partial", "stream", "duplicate", "reverse", "dq", "too_many"])
def test_failure_before_commit_leaves_old_day(env, failure):
    old = write(env)
    data = prepared(env, sales=(4,), stock=(5,))
    closed = []
    def stream():
        try:
            yield from data
            raise RuntimeError("source interrupted")
        finally:
            closed.append(True)
    batches = data
    if failure == "stream":
        batches = stream()
    elif failure == "duplicate":
        batches = data + data
    elif failure == "reverse":
        batches = [data[0].take(pa.array([1, 0]))]
    expected = 3 if failure == "partial" else 4 if failure == "duplicate" else 1 if failure == "too_many" else 2
    with pytest.raises((RuntimeError, ValueError)):
        write(env, batches, expected=expected, verify=lambda: failure != "dq")
    assert env[2].refresh().current_snapshot().snapshot_id == old["snapshot_id"]
    assert sorted(env[2].scan().to_arrow()["sku_id"].to_pylist()) == [1, 2, 3]
    if failure == "stream":
        assert closed == [True]


def test_resume_reads_full_day_and_returns_no_new_commit(env):
    old = write(env)
    result = resume(env)
    assert result["resumed"] and result["snapshot_id"] == old["snapshot_id"]
    assert result["inputs"] == VERSIONS and "dq_status" not in result
    proof = json.loads(env[2].refresh().current_snapshot().summary.additional_properties[WRITER.PROOF_KEY])
    assert len(proof["batches"]) == 2 and "batches" not in result


@pytest.mark.parametrize("change", ["snapshot", "uuid", "dq_run", "manifest", "version"])
def test_new_input_version_never_reuses_same_count(env, change):
    write(env)
    inputs = deepcopy(VERSIONS)
    args = {"inputs": inputs}
    if change == "snapshot":
        inputs["sales"]["snapshot_id"] += 1
    elif change == "uuid":
        inputs["stock"]["table_uuid"] = VERSIONS["sales"]["table_uuid"]
    elif change == "dq_run":
        inputs["sales"]["run_id"] = "new-dq-run"
    else:
        args[change] = "changed"
    assert resume(env, **args) is None


def test_resume_rejects_changed_payload_even_when_count_and_keys_equal(env):
    from pyiceberg.expressions import EqualTo

    write(env)
    data = pa.concat_tables(prepared(env))
    field = data.schema.field("sales_gmv")
    data = data.set_column(data.schema.get_field_index(field.name), field,
                           pa.array([Decimal(7), None, Decimal(7)], type=field.type))
    env[2].refresh().overwrite(data, overwrite_filter=EqualTo("date", DAY))
    assert resume(env) is None


def test_lost_acknowledgement_can_resume_without_rewriting(env, monkeypatch):
    original = CHECKPOINT.verify_proof
    def lost(*args):
        raise RuntimeError("lost acknowledgement")
    monkeypatch.setattr(CHECKPOINT, "verify_proof", lost)
    with pytest.raises(RuntimeError, match="acknowledgement"):
        write(env)
    snapshot = env[2].refresh().current_snapshot().snapshot_id
    monkeypatch.setattr(CHECKPOINT, "verify_proof", original)
    assert resume(env)["snapshot_id"] == snapshot


@pytest.mark.parametrize("column,value", [
    ("sales_snapshot_id", 999), ("stock_table_uuid", VERSIONS["sales"]["table_uuid"]),
    ("source_manifest_id", "wrong"), ("source_contract_version", "wrong"),
    ("sales_source_manifest_id", None), ("is_in_stock_eod", False),
    ("date", date(2026, 8, 31)), ("sales_component_present", False),
])
def test_mixed_or_missing_metadata_rejected_before_commit(env, column, value):
    data = pa.concat_tables(prepared(env))
    rows = data.to_pylist()
    index = 1 if column.startswith("stock_") or column == "is_in_stock_eod" else 0
    rows[index][column] = value
    with pytest.raises(ValueError):
        write(env, [pa.Table.from_pylist(rows, schema=data.schema)])
    assert env[2].refresh().current_snapshot() is None


@pytest.mark.parametrize("limit,value", [("max_batch_rows", 1), ("max_batch_bytes", 1),
                                       ("max_batch_rows", True), ("max_batch_bytes", 0)])
def test_limits_close_iterator_and_do_not_commit(env, limit, value):
    data = prepared(env)
    class Stream:
        closed = False
        def __iter__(self):
            return self
        def __next__(self):
            return data[0]
        def close(self):
            self.closed = True
    batches = Stream()
    env[0]["runtime"][limit] = value
    with pytest.raises(ValueError):
        write(env, batches)
    assert batches.closed and env[2].refresh().current_snapshot() is None


def test_changed_output_head_is_not_overwritten(env):
    from pyiceberg.expressions import EqualTo

    write(env)
    replacement = pa.concat_tables(prepared(env, sales=(8,), stock=(9,)))
    def concurrent():
        env[2].refresh().overwrite(replacement, overwrite_filter=EqualTo("date", DAY))
        return True
    with pytest.raises(RuntimeError, match="snapshot/UUID"):
        write(env, verify=concurrent)
    assert sorted(env[2].refresh().scan().to_arrow()["sku_id"].to_pylist()) == [8, 9]


def test_resume_after_neighbor_commit_reads_current_day(env):
    write(env)
    latest = write(env, day=date(2026, 8, 31))
    assert resume(env)["snapshot_id"] == latest["snapshot_id"]


def test_input_lineage_is_copied_before_iterator_changes_it(env):
    inputs = deepcopy(VERSIONS)
    def stream():
        inputs["sales"]["snapshot_id"] += 1
        yield from prepared(env)
    receipt = write(env, stream(), inputs=inputs)
    assert receipt["inputs"] == VERSIONS


@pytest.mark.parametrize("failure", ["missing", "partition", "schema", "identifier", "catalog",
                                     "count", "bool_count", "date", "callback", "inputs"])
def test_preflight_rejects_before_read_and_closes_stream(env, failure):
    class Stream:
        closed = False
        def __iter__(self):
            return self
        def __next__(self):
            pytest.fail("Нельзя читать источник при неверном preflight")
        def close(self):
            self.closed = True
    config, catalog, table = env
    options = {}
    if failure == "missing":
        config["table"]["name"] = "missing"
    elif failure == "partition":
        with table.update_spec() as spec:
            spec.remove_field("date")
    elif failure == "schema":
        with table.update_schema() as update:
            update.delete_column("sales_units")
    elif failure == "identifier":
        config["table"]["name"] = "gold.bad.name"
    elif failure == "catalog":
        config["table"]["catalog"] = "other"
    elif failure in ("count", "bool_count"):
        options["expected"] = 0 if failure == "count" else True
    elif failure == "date":
        options["day"] = NOW
    elif failure == "callback":
        options["verify"] = None
    else:
        options["inputs"] = {"sales": VERSIONS["sales"]}
    batches = Stream()
    with pytest.raises(ValueError):
        write(env, batches, **options)
    assert batches.closed and table.refresh().current_snapshot() is None


@pytest.mark.parametrize("result", [False, None, 1, "passed"])
def test_requires_explicit_true_input_verification(env, result):
    with pytest.raises(ValueError, match="upstream"):
        write(env, verify=lambda: result)
    assert env[2].refresh().current_snapshot() is None


def test_empty_source_day_cannot_erase_old_gold(env):
    old = write(env)
    with pytest.raises(ValueError, match="Неполный"):
        write(env, [])
    assert env[2].refresh().current_snapshot().snapshot_id == old["snapshot_id"]
