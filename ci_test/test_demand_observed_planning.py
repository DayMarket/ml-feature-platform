"""План gold: штатное окно 31 день, ручной диапазон и неизменность inputs/output."""

from datetime import date
from importlib import import_module

import pytest

from ci_test.test_demand_observed_inputs import payload
from ci_test.test_demand_observed_preparation import NOW, ROOT
from ci_test.test_demand_observed_runtime import setup as setup
from ci_test.test_demand_observed_writer import env as env, write

PLAN = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.planning")
FIRST = date(2026, 7, 1)
STOP = date(2026, 9, 9)
STATE = {"table_uuid": "11111111-1111-4111-8111-111111111111", "snapshot_id": 123}


def arguments(**changes):
    data = payload()
    refs = {kind: dict(ref, logical_date=NOW.isoformat()) for kind, ref in data[2].items()}
    args = dict(run_id="gold-run", mode="regular", interval_start="2026-09-08T05:00:00Z",
                interval_end="2026-09-09T05:00:00Z", history_start=FIRST, references=refs)
    return data[0], args | changes


def request(**changes):
    config, args = arguments(**changes)
    return PLAN.build_request(config, **args, output_state=STATE)


def test_regular_uses_exactly_last_31_days():
    result = request()
    assert result["dates"][0] == "2026-08-09" and len(result["dates"]) == 31
    assert result["dates"][-1] == "2026-09-08"
    config, _ = arguments()
    assert len(PLAN.validate_request(config, result)) == 31
    assert request() == result


def test_manual_uses_full_explicit_range():
    result = request(mode="manual", interval_start="2026-07-01T00:00:00Z")
    assert len(result["dates"]) == (STOP - FIRST).days
    assert result["history_start"] == "2026-07-01"
    assert PLAN.validate_request(arguments()[0], result)[0] == FIRST


@pytest.mark.parametrize("change", ["dates", "reference", "config", "omit_recent", "outside", "state"])
def test_tampered_plan_or_missing_mandatory_days_rejected(change):
    config, _ = arguments()
    result = request()
    if change == "config":
        config["runtime"]["refresh_days"] = 32
    elif change == "reference":
        result["references"]["sales"]["run_id"] = "different"
    elif change == "state":
        result["output_state"]["snapshot_id"] = 456
    else:
        if change == "dates":
            result["dates"].reverse()
        elif change == "omit_recent":
            result["dates"].pop()
        else:
            result["dates"].insert(0, "2020-01-01")
        result["request_id"] = PLAN.digest({k: v for k, v in result.items() if k != "request_id"})
    with pytest.raises(ValueError):
        PLAN.validate_request(config, result)


@pytest.mark.parametrize("end", ["2026-09-09T05:00:00", "2026-09-09T05:00:00Z",
    "2026-09-09 05:00:00+00:00", "2026-09-09T10:00:00+05:00"])
def test_airflow_time_normalization(end):
    assert request(interval_end=end)["interval_end"] == "2026-09-09T05:00:00+00:00"


def test_prepare_freezes_output(env):
    config, catalog, table = env
    _, args = arguments()
    write(env)
    snapshot = table.refresh().current_snapshot().snapshot_id
    result = PLAN.prepare_request(config, ROOT, catalog=catalog, query=lambda _: [], **args)
    assert len(result["dates"]) == 31
    assert result["output_state"] == {"table_uuid": str(table.metadata.table_uuid), "snapshot_id": snapshot}


def test_manual_plan_does_not_read_catalog():
    config, args = arguments(mode="manual", interval_start="2026-07-01T00:00:00Z")
    result = PLAN.prepare_request(config, ROOT, catalog=None, query=None, **args)
    assert result["output_state"] is None and len(result["dates"]) == (STOP - FIRST).days


@pytest.mark.parametrize("state", [None, {}, {"table_uuid": "bad", "snapshot_id": 1},
    {"table_uuid": STATE["table_uuid"], "snapshot_id": True}])
def test_regular_requires_proven_output_state(state):
    config, args = arguments()
    with pytest.raises(ValueError):
        PLAN.build_request(config, **args, output_state=state)


def test_invalid_owner_reference_blocks_before_catalog(env):
    config, catalog, _ = env
    _, args = arguments()
    args["references"]["stock"]["dag_id"] = args["references"]["sales"]["dag_id"]
    with pytest.raises(ValueError, match="владельцев"):
        PLAN.prepare_request(config, ROOT, catalog=catalog, query=lambda _: [], **args)
