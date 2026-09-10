"""Точные диапазоны E3: полный preflight, дневной resume и запрет частичного DQ."""

from copy import deepcopy
from datetime import timedelta
import json

import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, config, module
from ci_test.test_demand_restored_writer import bundle, target, prepared  # noqa: F401

ranges = module("restored", "ranges")


def request(cfg=None, *, selections=None, copy_id="copy-exact"):
    default = module("restored", "query").selection(run_id="e3-selected", prediction_date=DAY + timedelta(days=2),
                                                   start=DAY, end=DAY + timedelta(days=2))
    return ranges.build_request(cfg or config("restored"), copy_id=copy_id, selections=selections or [default])


def test_request_is_json_safe_exact_and_keeps_sparse_ranges():
    day = DAY - timedelta(days=200)
    first = module("restored", "query").selection(run_id="e3-selected", prediction_date=DAY + timedelta(days=2),
                                                 start=day, end=day + timedelta(days=1))
    recent = first | {"start": DAY, "end": DAY + timedelta(days=2)}
    value = request(selections=[first, recent])
    assert value["dates"] == [day.isoformat(), DAY.isoformat(), (DAY + timedelta(days=1)).isoformat()]
    assert ranges.validate_request(config("restored"), json.loads(json.dumps(value))) == [first, recent]


@pytest.mark.parametrize("change", ["run", "cutoff", "overlap", "reverse"])
def test_mixed_or_overlapping_ranges_block(change):
    first = ranges.validate_request(config("restored"), request())[0]
    second = first | {"start": DAY - timedelta(days=1), "end": DAY}
    values = [second, first]
    if change == "run":
        first["run_id"] = "other"
    elif change == "cutoff":
        first["prediction_date"] += timedelta(days=1)
    elif change == "overlap":
        second["end"] += timedelta(days=1)
    else:
        values.reverse()
    with pytest.raises(ValueError):
        request(selections=values)


@pytest.mark.parametrize("field", ["dates", "request_id", "config_digest", "prediction_date", "copy_id"])
def test_mutated_request_blocks(field):
    value = request()
    value[field] = [] if field == "dates" else "changed"
    with pytest.raises(ValueError):
        ranges.validate_request(config("restored"), value)


def test_config_change_requires_new_request():
    cfg = config("restored")
    value = request(cfg)
    cfg["source"]["contract_version"] = "new"
    with pytest.raises(ValueError, match="конфигурация"):
        ranges.validate_request(cfg, value)


@pytest.fixture
def scenario(target, monkeypatch):  # noqa: F811
    cfg, catalog, table = target
    value = request(cfg)
    days = [DAY, DAY + timedelta(days=1)]
    data = [bundle(day=day, kinds=("final", "provisional")) for day in days]
    passport = dict(data[0][1])
    block = json.loads(passport["output_manifest"])["fp_daily_copy"]
    block["days"] += json.loads(data[1][1]["output_manifest"])["fp_daily_copy"]["days"]
    passport["output_manifest"] = json.dumps({"fp_daily_copy": block})
    state = {"passport": passport, "writes": [], "fail_day": None, "hold": True}
    monkeypatch.setattr(ranges, "read_run", lambda *args: deepcopy(state["passport"]))

    def load(config, catalog, client, *, selected, day, manifest, require_run_held, expected_run):
        state["writes"].append(day)
        if day == state["fail_day"]:
            raise RuntimeError("source failure")
        assert expected_run == state["passport"]
        raw_batches = data[days.index(day)][2]
        batches = [module("restored", "preparation").prepare_batch(
            batch, table.schema().as_arrow(), selected=selected, run=expected_run,
            manifest=manifest, version=cfg["source"]["contract_version"], ingested_at=CAPTURE)
            for batch in raw_batches]
        return module("restored", "writer").write_day(
            config, catalog, batches, selected=selected, day=day, manifest=manifest, run=expected_run,
            version=cfg["source"]["contract_version"], ingested_at=CAPTURE,
            verify_source=lambda: require_run_held(selected, expected_run))

    monkeypatch.setattr(ranges, "load_day", load)
    return cfg, catalog, table, value, state


def transfer(scenario, **changes):
    cfg, catalog, _, value, state = scenario
    kwargs = {"preflight": lambda *args: True, "require_run_held": lambda *args: state["hold"]}
    return ranges.load_range(cfg, catalog, None, value, **(kwargs | changes))


def test_all_days_preflighted_before_first_write(scenario):
    _, _, table, _, state = scenario
    block = json.loads(state["passport"]["output_manifest"])
    block["fp_daily_copy"]["days"].pop()
    state["passport"]["output_manifest"] = json.dumps(block)
    with pytest.raises(ValueError, match="не покрывает"):
        transfer(scenario)
    assert state["writes"] == [] and table.refresh().current_snapshot() is None


@pytest.mark.parametrize("failure", ["service", "hold"])
def test_missing_service_or_hold_never_writes(scenario, failure):
    state = scenario[-1]
    state["hold"] = failure != "hold"
    with pytest.raises(ValueError):
        transfer(scenario, preflight=lambda *args: failure != "service")
    assert not state["writes"]


def test_restart_resumes_first_committed_day_and_rechecks_range(scenario):
    cfg, _, table, value, state = scenario
    state["fail_day"] = DAY + timedelta(days=1)
    with pytest.raises(RuntimeError, match="source failure"):
        transfer(scenario)
    assert table.refresh().scan().count() == 2
    state["fail_day"] = None
    written = transfer(scenario)
    assert state["writes"] == [DAY, DAY + timedelta(days=1), DAY + timedelta(days=1)]
    assert [d["resumed"] for d in written["day_receipts"]] == [True, False]
    assert written["status"] == "written" and "dq_status" not in written
    again = transfer(scenario)
    assert all(d["resumed"] for d in again["day_receipts"])
    assert written["snapshot_id"] == again["snapshot_id"]
    assert set(table.refresh().refs()) == {"main"}
    checks = dq_checks(written)
    assert ranges.require_range_dq(cfg, value, written, checks)["status"] == "ready"


def test_passport_change_between_days_blocks_instead_of_mixing(scenario, monkeypatch):
    state = scenario[-1]
    original = ranges.load_day
    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        state["passport"]["state_version"] += 1
        return result
    monkeypatch.setattr(ranges, "load_day", changed)
    with pytest.raises(ValueError, match="между днями"):
        transfer(scenario)
    assert state["writes"] == [DAY]
    assert scenario[2].refresh().scan().count() == 2


def dq_checks(written):
    return [{"date": receipt["date"], "source_manifest_id": receipt["source_manifest_id"],
             "source_day": deepcopy(receipt["source_day"]), "table_uuid": written["table_uuid"],
             "dq_status": "passed", "snapshot_id": written["snapshot_id"],
             "request_id": written["request_id"], "rows_checked": receipt["rows_written"]}
            for receipt in written["day_receipts"]]


@pytest.mark.parametrize("field,value", [("date", "2000-01-01"), ("dq_status", "missing"),
    ("snapshot_id", 123), ("request_id", "other"), ("source_day", {}), ("rows_checked", 1),
    ("rows_checked", True), ("source_manifest_id", "other"), ("table_uuid", "other")])
def test_wrong_dq_receipt_never_approves_range(scenario, field, value):
    written = transfer(scenario)
    checks = dq_checks(written)
    checks[0][field] = value
    with pytest.raises(ValueError, match="точную запись"):
        ranges.require_range_dq(scenario[0], scenario[3], written, checks)


def test_missing_day_dq_blocks(scenario):
    written = transfer(scenario)
    with pytest.raises(ValueError, match="каждого дня"):
        ranges.require_range_dq(scenario[0], scenario[3], written, dq_checks(written)[1:])


def test_dq_cannot_approve_mixed_source_states_even_if_checks_match(scenario):
    written = transfer(scenario)
    written["day_receipts"][0]["source_day"]["source_state_version"] += 1
    with pytest.raises(ValueError, match="источник receipt"):
        ranges.require_range_dq(scenario[0], scenario[3], written, dq_checks(written))


def test_resume_still_requires_hold(scenario):
    transfer(scenario)
    scenario[-1]["hold"] = False
    with pytest.raises(ValueError, match="не удерживается"):
        transfer(scenario)
    assert len(scenario[-1]["writes"]) == 2


def test_range_does_not_fill_gap_or_remove_neighbor(scenario):
    cfg, catalog, table, _, state = scenario
    first = ranges.validate_request(cfg, request(cfg))[0]
    previous = first | {"start": DAY - timedelta(days=3), "end": DAY - timedelta(days=2)}
    value = request(cfg, selections=[previous, first])
    old = bundle(day=previous["start"], kinds=("final", "provisional"))
    document = json.loads(state["passport"]["output_manifest"])
    document["fp_daily_copy"]["days"] = json.loads(old[1]["output_manifest"])["fp_daily_copy"]["days"] + document["fp_daily_copy"]["days"]
    state["passport"]["output_manifest"] = json.dumps(document)
    expected = ranges.day_manifest(cfg, previous, state["passport"], previous["start"])
    # Старый отдельный день уже перенесён тем же запросом; после retry нужен только read-back.
    batches = [module("restored", "preparation").prepare_batch(
        raw, table.schema().as_arrow(), selected=previous, run=state["passport"],
        manifest=ranges.copy_manifest(value, previous["start"]),
        version=cfg["source"]["contract_version"], ingested_at=CAPTURE) for raw in old[2]]
    module("restored", "writer").write_day(
        cfg, catalog, batches, selected=previous, day=previous["start"], run=state["passport"],
        manifest=ranges.copy_manifest(value, previous["start"]), version=cfg["source"]["contract_version"],
        ingested_at=CAPTURE, verify_source=lambda: True)
    outcome = transfer((cfg, catalog, table, value, state))
    assert outcome["day_receipts"][0]["source_day"] == expected
    assert outcome["day_receipts"][0]["resumed"] is True
    assert table.refresh().scan().count() == 6
    assert set(table.scan().to_arrow()["date"].to_pylist()) == {previous["start"], DAY, DAY + timedelta(days=1)}
