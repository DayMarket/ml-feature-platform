"""SKU planner фиксирует dates/reference/output state и возобновляет только свой запрос."""

from copy import deepcopy
from datetime import date, timedelta
from importlib import import_module

import pytest

from ci_test.test_demand_sku_sales_from_fp import case as case, env as env, ROOT, CAPTURE, DAY, load, two_days

PLAN = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_planning")
RUNTIME = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_runtime")
REQUESTS = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_requests")
FIRST, STOP = date(2026, 7, 1), date(2026, 9, 9)
STATE = {"table_uuid": "11111111-1111-4111-8111-111111111111", "snapshot_id": 123}


def arguments(case, **changes):
    return dict(run_id="sku-run", mode="regular", interval_start="2026-09-08T04:00:00Z",
        interval_end="2026-09-09T04:00:00Z", history_start=FIRST,
        reference=dict(case[3], logical_date=CAPTURE.isoformat())) | changes


def test_regular_31_days_and_full_manual_range(case):
    request = PLAN.build_request(case[0], **arguments(case), output_state=STATE)
    assert request["dates"] == [(STOP - timedelta(days=n)).isoformat() for n in range(31, 0, -1)]
    assert len(PLAN.validate_request(case[0], request)) == 31
    manual = PLAN.build_request(case[0], **arguments(case, mode="manual", interval_start="2026-07-01T00:00:00Z"))
    assert len(PLAN.validate_request(case[0], manual)) == (STOP - FIRST).days


@pytest.mark.parametrize("change", ["date", "reference", "state", "config"])
def test_modified_plan_rejected(case, change):
    request = PLAN.build_request(case[0], **arguments(case), output_state=STATE)
    if change == "date":
        request["dates"].pop()
        request["request_id"] = PLAN.digest({k: v for k, v in request.items() if k != "request_id"})
    elif change == "reference":
        request["reference"]["run_id"] = "other"
    elif change == "state":
        request["output_state"]["snapshot_id"] = 456
    else:
        case[0]["runtime"]["refresh_days"] = 20
    with pytest.raises(ValueError):
        PLAN.validate_request(case[0], request)


def test_regular_preparation_freezes_output_state(case):
    request = PLAN.prepare_request(case[0], ROOT, catalog=case[1], query=lambda _: [], **arguments(case))
    assert len(request["dates"]) == 31 and request["output_state"]["snapshot_id"] is None


def runtime_plan(case):
    days = two_days(case)
    state = {"table_uuid": str(case[2].metadata.table_uuid), "snapshot_id": None}
    return PLAN.build_request(case[0], **arguments(case, history_start=DAY,
        interval_start=days[0].isoformat() + "T04:00:00Z", interval_end=(days[-1] + timedelta(days=1)).isoformat() + "T04:00:00Z"),
        output_state=state)


def execute(case, request):
    return PLAN.execute_request(case[0], ROOT, case[1], case[-1], request,
        fetch_checked=lambda ref: deepcopy(case[4]) if ref == case[3] else None, ingested_at=CAPTURE)


def test_retry_continues_own_commits_from_frozen_plan(case, monkeypatch):
    request = runtime_plan(case)
    original = RUNTIME.load_day
    def fail(*args, **kwargs):
        if kwargs["day"] != DAY:
            raise RuntimeError("interrupted")
        return original(*args, **kwargs)
    with monkeypatch.context() as context:
        context.setattr(RUNTIME, "load_day", fail)
        with pytest.raises(RuntimeError, match="interrupted"):
            execute(case, request)
    result = execute(case, request)
    assert [r["resumed"] for r in result["day_receipts"]] == [True, False]
    assert result["request_id"] == request["request_id"]


def test_foreign_write_after_plan_blocks_before_source_query(case):
    request = runtime_plan(case)
    load(case)
    case[-1].queries.clear()
    with pytest.raises(ValueError, match="coverage"):
        execute(case, request)
    assert not case[-1].queries


@pytest.mark.parametrize("end", ["2026-09-09T04:00:00", "2026-09-09T04:00:00Z",
    "2026-09-09 04:00:00", "2026-09-09 04:00:00+00:00", "2026-09-09T09:00:00+05:00"])
def test_owner_utc_formats(case, end):
    args = REQUESTS.owner_arguments(case[0], {}, run_id="run", interval_start="2026-09-08T04:00:00Z",
        interval_end=end, run_after=CAPTURE, run_type="scheduled",
        reference=dict(case[3], logical_date=CAPTURE.isoformat()))
    assert args["interval_end"] == "2026-09-09T04:00:00+00:00"


def test_explicit_manual_range_ignores_airflow_interval(case):
    conf = {"mode": "manual", "start": "2022-09-01", "end": "2022-09-03",
            "reference": dict(case[3], logical_date=CAPTURE.isoformat())}
    args = REQUESTS.owner_arguments(case[0], conf, run_id="r", run_type="manual",
        interval_start=None, interval_end=None, run_after=CAPTURE)
    assert args["interval_start"] == "2022-09-01T00:00:00+00:00"
    assert args["interval_end"] == "2022-09-03T00:00:00+00:00"
