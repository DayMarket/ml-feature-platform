"""Проверить атомарную замену полного E3-дня и fail-closed source manifest."""

from datetime import timedelta
from hashlib import sha256
import json

import pyarrow as pa
import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, config, module, raw, run, schema


@pytest.fixture
def target(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    cfg = config("restored")
    table = catalog.create_table(module("restored", "preparation").target_ref(cfg, catalog.name),
                                 schema=schema("restored"))
    with table.update_spec() as spec:
        spec.add_identity("date")
    yield cfg, catalog, table
    catalog.engine.dispose()


def bundle(*, kinds=("provisional",), run_id="e3-selected", day=DAY, changes=None):
    selected = module("restored", "query").selection(run_id=run_id, prediction_date=DAY + timedelta(days=2),
                                                   start=day, end=day + timedelta(days=1))
    batches = [raw("restored", {"date": day, "run_id": run_id, "estimate_kind": kind} | (changes or {}))
               for kind in sorted(kinds)]
    digest = module("restored", "manifest").ContentDigest()
    for batch in batches:
        digest.update(batch)
    entry = {"date": day.isoformat(), "rows": digest.rows, "estimate_counts": dict(digest.counts),
             "sha256": digest.hexdigest()}
    passport = run() | {"run_id": run_id, "output_manifest": json.dumps({"fp_daily_copy": {
        "version": 1, "checksum_algorithm": "e3_daily_rows_sha256_v1",
        "table": "sku_sales_forecast.demand_forecast_demand_panel",
        "run_id": run_id, "prediction_date": selected["prediction_date"].isoformat(), "days": [entry],
    }})}
    return selected, passport, batches


def prepared(target, data):
    selected, passport, batches = data
    return [module("restored", "preparation").prepare_batch(
        batch, target[2].schema().as_arrow(), selected=selected, run=passport,
        manifest="copy-1", version="v1", ingested_at=CAPTURE) for batch in batches]


def write(target, data, *, batches=None, verify=lambda: True):
    selected, passport, _ = data
    return module("restored", "writer").write_day(
        target[0], target[1], prepared(target, data) if batches is None else batches,
        day=selected["start"], selected=selected, run=passport,
        manifest="copy-1", version="v1", ingested_at=CAPTURE, verify_source=verify)


def resume(target, data):
    selected, passport, _ = data
    return module("restored", "checkpoint").resume_day(
        target[0], target[1], day=selected["start"], selected=selected, run=passport,
        manifest="copy-1", version="v1")


def test_final_only_replaces_old_run_and_keeps_other_day(target):
    earlier = DAY - timedelta(days=1)
    write(target, bundle(day=earlier))
    write(target, bundle(kinds=("provisional", "final")))
    data = bundle(kinds=("final",), run_id="new-final")
    receipt = write(target, data)
    rows = target[2].refresh().scan().to_arrow().to_pylist()
    assert {(r["date"], r["run_id"], r["estimate_kind"]) for r in rows} == {
        (earlier, "e3-selected", "provisional"), (DAY, "new-final", "final")}
    assert receipt["status"] == "written" and "dq_status" not in receipt
    assert set(target[2].refs()) == {"main"}
    assert resume(target, data)["resumed"]
    assert resume(target, bundle(kinds=("provisional", "final"))) is None


def test_unavailable_nulls_survive(target):
    data = bundle(changes={"quality_status": "unavailable", "unavailable_reason": "missing_price",
                           "lost_units": None, "lost_gmv": None, "p_active": None})
    write(target, data)
    row = target[2].refresh().scan().to_arrow().to_pylist()[0]
    assert row["quality_status"] == "unavailable" and row["unavailable_reason"] == "missing_price"
    assert row["lost_units"] is None and row["lost_gmv"] is None and row["p_active"] is None


@pytest.mark.parametrize("failure", ["partial", "hold", "stream", "checksum", "mixed_run", "duplicate"])
def test_failure_preserves_previous_day(target, failure):
    old = write(target, bundle())
    data = bundle(kinds=("final", "provisional"), run_id="new-run")
    batches = prepared(target, data)
    if failure == "partial":
        batches = batches[:1]
    elif failure == "checksum":
        field = "lost_units"
        batch = batches[0]
        batches[0] = batch.set_column(batch.schema.get_field_index(field), batch.schema.field(field), pa.array([9.0]))
    elif failure == "mixed_run":
        batch = batches[0]
        field = batch.schema.field("run_id")
        batches[0] = batch.set_column(batch.schema.get_field_index("run_id"), field, pa.array(["foreign"], type=field.type))
    elif failure == "duplicate":
        batches = [batches[0], batches[0]]
    elif failure == "stream":
        first = batches[0]
        def broken():
            yield first
            raise RuntimeError("source interrupted")
        batches = broken()
    with pytest.raises((ValueError, RuntimeError)):
        write(target, data, batches=batches, verify=lambda: failure != "hold")
    assert target[2].refresh().current_snapshot().snapshot_id == old["snapshot_id"]


def test_resume_after_lost_ack_and_no_new_commit(target, monkeypatch):
    def lost(*args):
        raise RuntimeError("lost ACK")
    monkeypatch.setattr(module("restored", "writer"), "verify_proof", lost)
    data = bundle(kinds=("final",))
    with pytest.raises(RuntimeError, match="lost ACK"):
        write(target, data)
    snapshot = target[2].refresh().current_snapshot().snapshot_id
    receipt = resume(target, data)
    assert receipt["snapshot_id"] == snapshot and receipt["resumed"]


def test_resume_not_reused_after_contents_changed_with_same_count(target):
    from pyiceberg.expressions import EqualTo
    data = bundle()
    write(target, data)
    changed = prepared(target, data)[0]
    field = changed.schema.field("lost_units")
    changed = changed.set_column(changed.schema.get_field_index("lost_units"), field, pa.array([9.0]))
    target[2].refresh().overwrite(changed, overwrite_filter=EqualTo("date", DAY))
    assert resume(target, data) is None


def test_manifest_checksum_independent_of_chunking_and_arrow_types():
    data = bundle(kinds=("provisional", "final"))
    batches = data[2]
    first = module("restored", "manifest").ContentDigest()
    second = module("restored", "manifest").ContentDigest()
    for batch in batches:
        first.update(batch)
    second.update(pa.concat_tables(batches))
    assert first.hexdigest() == second.hexdigest()
    assert first.counts == second.counts == {"final": 1, "provisional": 1}


def test_wire_format_has_independent_reference():
    # Массив контракта задан независимо от RAW_COLUMNS и row_bytes.
    values = [DAY.isoformat(), 1, "provisional", "e3-selected", "2026-09-08"]
    values += ["0x1.0000000000000p+0"] * 11
    values += ["UZS", "v1", 1, None, "v1", "ok", "", "2026-09-09T04:00:00.000000Z"]
    payload = json.dumps(values, ensure_ascii=True, separators=(",", ":")).encode()
    expected = sha256(b"e3_daily_rows_sha256_v1\n" + len(payload).to_bytes(8, "big") + payload).hexdigest()
    digest = module("restored", "manifest").ContentDigest()
    digest.update(raw("restored"))
    assert digest.hexdigest() == expected


@pytest.mark.parametrize("bad", ["missing", "version", "algorithm", "table", "run", "cutoff", "day",
                                  "duplicate_day", "counts", "bool_count", "empty_counts", "hash", "coverage"])
def test_invalid_manifest_blocks_before_write(target, bad):
    selected, passport, batches = bundle()
    document = json.loads(passport["output_manifest"])
    block = document["fp_daily_copy"]
    if bad in {"version", "algorithm", "table", "run", "cutoff"}:
        key = {"algorithm": "checksum_algorithm", "run": "run_id", "cutoff": "prediction_date"}.get(bad, bad)
        block[key] = True if bad == "version" else "incorrect"
    elif bad == "missing":
        document = {"outputs": 1}
    elif bad == "day":
        block["days"][0]["date"] = "2026-09-08"
    elif bad == "duplicate_day":
        block["days"] *= 2
    elif bad == "counts":
        block["days"][0]["estimate_counts"] = {"final": 2}
    elif bad == "bool_count":
        block["days"][0]["estimate_counts"] = {"final": True}
    elif bad == "empty_counts":
        block["days"][0]["estimate_counts"] = {}
    elif bad == "hash":
        block["days"][0]["sha256"] = ""
    elif bad == "coverage":
        selected["start"] -= timedelta(days=1)
    passport["output_manifest"] = json.dumps(document)
    with pytest.raises(ValueError):
        write(target, (selected, passport, batches), batches=[])
    assert target[2].refresh().current_snapshot() is None


def test_same_count_wrong_kind_rejected(target):
    selected, passport, batches = bundle()
    document = json.loads(passport["output_manifest"])
    document["fp_daily_copy"]["days"][0]["estimate_counts"] = {"final": 1}
    passport["output_manifest"] = json.dumps(document)
    with pytest.raises(ValueError, match="counts/checksum"):
        write(target, (selected, passport, batches))
    assert target[2].refresh().current_snapshot() is None


def test_resume_rejects_new_state_even_with_same_rows(target):
    data = bundle()
    write(target, data)
    data[1]["state_version"] += 1
    assert resume(target, data) is None
