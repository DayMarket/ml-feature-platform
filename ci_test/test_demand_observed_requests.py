"""Параметры scheduled и manual запусков gold owner."""

from datetime import date
from importlib import import_module

import pytest

from ci_test.test_demand_observed_planning import FIRST, STOP, arguments
from ci_test.test_demand_observed_preparation import NOW

REQUESTS = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.requests")


def owner(conf=None, **changes):
    config, args = arguments()
    args.pop("mode")
    args.pop("history_start")
    references = args.pop("references")
    defaults = {"run_type": "scheduled", "run_after": NOW, "references": references}
    return REQUESTS.owner_arguments(config, conf or {}, **(args | defaults | changes))


def manual_conf():
    _, args = arguments()
    return {
        "mode": "manual",
        "start": FIRST.isoformat(),
        "end": STOP.isoformat(),
        "references": args["references"],
    }


def test_scheduled_uses_configured_floor_and_airflow_interval():
    result = owner()
    assert result["mode"] == "regular"
    assert result["history_start"] == date(2022, 9, 1)
    assert result["interval_end"] == "2026-09-09T05:00:00+00:00"


def test_manual_uses_explicit_first_to_first_range():
    conf = manual_conf()
    result = owner(
        conf,
        run_type="manual",
        references=None,
        interval_start=None,
        interval_end=None,
    )
    assert result["mode"] == "manual"
    assert result["history_start"] == FIRST
    assert result["interval_start"] == "2026-07-01T00:00:00+00:00"
    assert result["interval_end"] == "2026-09-09T00:00:00+00:00"
    assert result["references"] == conf["references"]


@pytest.mark.parametrize(
    "conf,changes",
    [
        ({"mode": "manual", "start": FIRST.isoformat(), "end": STOP.isoformat()}, {"run_type": "manual", "references": None}),
        ({"mode": "manual", "start": "2026-09-09", "end": "2026-09-09", "references": {}}, {"run_type": "manual", "references": None}),
        ({"mode": "regular"}, {"run_type": "manual"}),
        ({"mode": "manual", "start": FIRST.isoformat(), "end": STOP.isoformat(), "references": {}}, {}),
        ({"skip_dq": True}, {}),
    ],
)
def test_invalid_arguments_fail_before_io(conf, changes):
    with pytest.raises(ValueError):
        owner(conf, **changes)
