"""Дневной SKU loader читает exact seller snapshot и сохраняет прежний день при отказе."""

from copy import deepcopy
from datetime import timedelta
from importlib import import_module
from pathlib import Path

import pyarrow as pa
import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, config, schema
from ci_test.test_demand_seller_sales_writer import env as env, mod, prepared, raw, shared

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "layers.silver.sku_id.demand_sales_daily.v1.job."
runtime = import_module(PACKAGE + "seller_runtime")
inputs = import_module(PACKAGE + "seller_inputs")


class Connection:
    def __init__(self, batch):
        self.batch = batch
        self.queries, self.cursors = [], []
        self.counts = None
        self.bad_type = False
        self.fail_fetch = False

    def cursor(self):
        owner = self
        class Cursor:
            closed = False
            def execute(self, sql):
                owner.queries.append(sql)
                self.is_count = sql.startswith('SELECT count(DISTINCT "sku_id")')
                self.offset = 0
                selected = [row for row in owner.batch.to_pylist() if f"DATE '{row['date'].isoformat()}'" in sql]
                if self.is_count:
                    self.rows = owner.counts if owner.counts is not None else [[len({r["sku_id"] for r in selected}), len(selected)]]
                else:
                    columns = owner.batch.column_names
                    self.rows = [tuple(row[n] for n in columns) for row in selected]
                    types = {pa.date32(): "date", pa.int64(): "bigint", pa.decimal128(38, 0): "decimal(38,0)",
                             pa.timestamp("us"): "timestamp(6)", pa.float64(): "double"}
                    self.description = [(f.name, types.get(f.type, "varchar"), None, None, None, None, f.nullable)
                                        for f in owner.batch.schema]
                    if owner.bad_type:
                        self.description[0] = (columns[0], "varchar", None, None, None, None, True)
            def fetchmany(self, size):
                if owner.fail_fetch and not self.is_count and self.offset:
                    raise RuntimeError("broken stream")
                rows = self.rows[self.offset:self.offset + size]
                self.offset += len(rows)
                return rows
            def close(self):
                self.closed = True
        cursor = Cursor()
        self.cursors.append(cursor)
        return cursor


@pytest.fixture
def case(env):
    source, catalog, seller = env
    cfg = config("sales")
    cfg["runtime"]["max_batch_rows"] = 1
    target = catalog.create_table((cfg["table"]["schema"], cfg["table"]["name"]), schema=schema("sales"))
    with target.update_spec() as spec:
        spec.add_identity("date")
    batches = [prepared(row, seller.schema().as_arrow()) for row in shared()]
    for i, batch in enumerate(batches):
        name = "source_contract_version"
        batches[i] = batch.set_column(batch.schema.get_field_index(name), batch.schema.field(name),
                                     pa.array([source["source"]["contract_version"]], type=batch[name].type))
    receipt = mod("writer").write_day(source, catalog, batches, day=DAY, expected_rows=3,
        manifest="capture-1", version=source["source"]["contract_version"], ingested_at=CAPTURE,
        verify_source=lambda: True)
    reference = {"dag_id": source["dag"]["id"], "run_id": "seller-exact"}
    written = {"status": "written", "request_id": "seller-request", "table_uuid": receipt["table_uuid"],
               "snapshot_id": receipt["snapshot_id"], "dates": [DAY.isoformat()], "day_receipts": [receipt]}
    check = {"date": DAY.isoformat(), "dq_status": "passed", **reference,
             "request_id": written["request_id"], "snapshot_id": written["snapshot_id"],
             "table_uuid": written["table_uuid"], "source_manifest_id": receipt["source_manifest_id"],
             "rows_checked": 3}
    checked = {"dq_status": "passed", **reference, "receipt": written, "day_checks": [check]}
    return cfg, catalog, target, reference, checked, Connection(pa.concat_tables(batches))


def load(case, *, manifest="sku-output", fetch=None):
    cfg, catalog, _, ref, checked, connection = case
    return runtime.load_day(cfg, ROOT, catalog, connection, day=DAY, reference=ref,
        fetch_checked=fetch or (lambda actual: deepcopy(checked) if actual == ref else None),
        manifest=manifest, ingested_at=CAPTURE)


def test_load_exact_snapshot_and_resume_without_sql(case):
    cfg, catalog, target, reference, checked, connection = case
    result = load(case)
    target.refresh()
    rows = target.scan().to_arrow().to_pylist()
    assert len(rows) == 1 and rows[0]["sales_orders"] == 2 and rows[0]["sales_order_items"] == 3
    binding = result["source_binding"]
    assert binding["run_id"] == reference["run_id"]
    assert binding["snapshot_id"] == checked["receipt"]["snapshot_id"]
    assert binding["schema_id"] is not None and len(binding["query_sha256"]) == 64
    assert all(f'FOR VERSION AS OF {binding["snapshot_id"]}' in sql for sql in connection.queries)
    assert all(cursor.closed for cursor in connection.cursors)
    connection.queries.clear()
    resumed = load(case)
    assert resumed["resumed"] and resumed["snapshot_id"] == result["snapshot_id"]
    assert not connection.queries


@pytest.mark.parametrize("kind", ["count", "metadata", "stream", "changed_dq", "bad_controls"])
def test_failures_preserve_old_sku_day(case, kind):
    load(case)
    _, _, target, _, checked, connection = case
    target.refresh()
    before = target.current_snapshot().snapshot_id
    fetch = None
    if kind == "count":
        connection.counts = [[1, 2]]
    elif kind == "metadata":
        connection.bad_type = True
    elif kind == "stream":
        connection.fail_fetch = True
    elif kind == "bad_controls":
        name = "sku_sales_orders"
        batch = connection.batch
        connection.batch = batch.set_column(batch.schema.get_field_index(name), batch.schema.field(name),
                                             pa.array([2, 3, 2], type=pa.int64()))
    else:
        calls = []
        def fetch(_reference):
            calls.append(1)
            return deepcopy(checked) if len(calls) == 1 else dict(checked, dq_status="failed")
    with pytest.raises((ValueError, RuntimeError)):
        load(case, manifest="another-output", fetch=fetch)
    target.refresh()
    assert target.current_snapshot().snapshot_id == before
    assert all(cursor.closed for cursor in connection.cursors)


@pytest.mark.parametrize("field,value", [("run_id", "foreign"), ("dq_status", "failed")])
def test_missing_exact_dq_prevents_source_sql(case, field, value):
    checked = deepcopy(case[4])
    checked[field] = value
    with pytest.raises(ValueError):
        load(case, fetch=lambda _: checked)
    assert not case[-1].queries


def test_missing_snapshot_never_uses_latest(case):
    checked = deepcopy(case[4])
    checked["receipt"]["snapshot_id"] = checked["day_checks"][0]["snapshot_id"] = 1
    with pytest.raises(ValueError, match="latest"):
        load(case, fetch=lambda _: checked)
    assert not case[-1].queries


def test_changed_input_run_does_not_reuse_output(case):
    first = load(case)
    cfg, catalog, target, reference, checked, connection = case
    reference["run_id"] = checked["run_id"] = checked["day_checks"][0]["run_id"] = "seller-other-exact"
    result = load(case)
    assert not result["resumed"] and result["snapshot_id"] != first["snapshot_id"]
    assert result["source_binding"]["run_id"] == "seller-other-exact"


def test_newer_current_does_not_replace_selected_schema_snapshot(case):
    cfg, catalog, _, _, checked, connection = case
    source, expected = inputs.source_config(cfg, ROOT)
    seller = catalog.load_table((source["table"]["schema"], source["table"]["name"]))
    from pyiceberg.types import LongType
    with seller.update_schema() as update:
        update.add_column("future_only", LongType())
    seller.refresh()
    assert len(seller.schema().fields) == 43
    result = load(case)
    assert result["source_binding"]["snapshot_id"] == checked["receipt"]["snapshot_id"]
    assert all('"future_only"' not in sql for sql in connection.queries)


def test_missing_history_day_blocks_binding(case):
    cfg, _, _, reference, checked, _ = case
    source, _ = inputs.source_config(cfg, ROOT)
    with pytest.raises(ValueError, match="полного"):
        inputs.bind_source(source, reference, checked, days=[DAY - timedelta(days=1), DAY], captured_at=CAPTURE)


def test_concurrent_target_after_source_preflight_is_not_overwritten(case, monkeypatch):
    load(case)
    target = case[2]
    target.refresh()
    payload = target.scan().to_arrow()
    read = runtime.read_count
    concurrent = []
    def change(*args, **kwargs):
        count = read(*args, **kwargs)
        target.overwrite(payload)
        concurrent.append(target.current_snapshot().snapshot_id)
        return count
    monkeypatch.setattr(runtime, "read_count", change)
    with pytest.raises(ValueError, match="preflight"):
        load(case, manifest="new-request")
    target.refresh()
    assert target.current_snapshot().snapshot_id == concurrent[0]


def test_changed_source_binding_cannot_be_lost_in_checkpoint(case):
    result = load(case)
    cfg, catalog, _, _, _, _ = case
    checkpoint = import_module(PACKAGE + "checkpoint")
    other = deepcopy(result["source_binding"])
    other["snapshot_id"] += 1
    assert checkpoint.resume_day(cfg, catalog, day=DAY, manifest="sku-output",
        version=cfg["source"]["contract_version"], source_binding=other) is None


def two_days(case):
    cfg, catalog, _, _, checked, connection = case
    source, _ = inputs.source_config(cfg, ROOT)
    seller = catalog.load_table((source["table"]["schema"], source["table"]["name"]))
    day = DAY + timedelta(days=1)
    batch = prepared(raw(date=day), seller.schema().as_arrow(), day=day)
    name = "source_contract_version"
    batch = batch.set_column(batch.schema.get_field_index(name), batch.schema.field(name),
                             pa.array([source["source"]["contract_version"]], type=batch[name].type))
    receipt = mod("writer").write_day(source, catalog, [batch], day=day, expected_rows=1,
        manifest="capture-1", version=source["source"]["contract_version"], ingested_at=CAPTURE,
        verify_source=lambda: True)
    checked["receipt"]["snapshot_id"] = receipt["snapshot_id"]
    checked["receipt"]["dates"].append(day.isoformat())
    checked["receipt"]["day_receipts"].append(receipt)
    checked["day_checks"][0]["snapshot_id"] = receipt["snapshot_id"]
    checked["day_checks"].append(dict(checked["day_checks"][0], date=day.isoformat(), rows_checked=1))
    connection.batch = pa.concat_tables([connection.batch, batch])
    return [DAY, day]


def load_range(case, days):
    cfg, catalog, _, reference, checked, connection = case
    return runtime.load_range(cfg, ROOT, catalog, connection, days=days, reference=reference,
        fetch_checked=lambda _: deepcopy(checked), manifest="range-output", request_id="range-request",
        ingested_at=CAPTURE)


def test_full_range_and_resume(case):
    days = two_days(case)
    written = load_range(case, days)
    assert written["dates"] == [day.isoformat() for day in days]
    assert [r["rows_written"] for r in written["day_receipts"]] == [1, 1]
    resumed = load_range(case, days)
    assert resumed["snapshot_id"] == written["snapshot_id"]
    assert all(r["resumed"] for r in resumed["day_receipts"])


@pytest.mark.parametrize("existing_later_day", [False, True])
def test_missing_later_day_preserves_completed_and_previous_days_then_resumes(case, existing_later_day):
    days = two_days(case)
    cfg, catalog, target, reference, checked, connection = case
    if existing_later_day:
        runtime.load_day(cfg, ROOT, catalog, connection, day=days[1], reference=reference,
            fetch_checked=lambda _: deepcopy(checked), manifest="previous-output", ingested_at=CAPTURE)
    target.refresh()
    previous_rows = target.scan().to_arrow().to_pylist()
    complete_source = connection.batch
    connection.batch = complete_source.slice(0, 3)
    with pytest.raises(ValueError, match="count"):
        load_range(case, days)
    target.refresh()
    rows = target.scan().to_arrow().to_pylist()
    first_rows = [row for row in rows if row["date"] == days[0]]
    assert len(first_rows) == 1
    assert first_rows[0]["sales_orders"] == 2 and first_rows[0]["sales_order_items"] == 3
    assert first_rows[0]["source_manifest_id"] == "range-output"
    assert [row for row in rows if row["date"] == days[1]] == previous_rows
    assert all(cursor.closed for cursor in connection.cursors)

    connection.batch = complete_source
    connection.queries.clear()
    resumed = load_range(case, days)
    assert resumed["dates"] == [day.isoformat() for day in days]
    assert [receipt["resumed"] for receipt in resumed["day_receipts"]] == [True, False]
    assert all(f"DATE '{days[0].isoformat()}'" not in sql for sql in connection.queries)
    target.refresh()
    rows = target.scan().to_arrow().to_pylist()
    assert [row for row in rows if row["date"] == days[0]] == first_rows
    assert len(rows) == 2 and {row["date"] for row in rows} == set(days)
    assert {row["source_manifest_id"] for row in rows} == {"range-output"}


def test_retry_resumes_finished_day_after_partial_range(case, monkeypatch):
    days = two_days(case)
    original = runtime.load_day
    def fail(*args, **kwargs):
        if kwargs["day"] == days[1]:
            raise RuntimeError("second day failed")
        return original(*args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(runtime, "load_day", fail)
        with pytest.raises(RuntimeError, match="second"):
            load_range(case, days)
    resumed = load_range(case, days)
    assert [r["resumed"] for r in resumed["day_receipts"]] == [True, False]


def test_range_bad_limits_prevent_io(case):
    case[0]["runtime"]["max_batch_rows"] = 0
    with pytest.raises(ValueError, match="лимиты"):
        load_range(case, [DAY])
    assert not case[-1].queries
