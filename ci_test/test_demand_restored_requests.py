"""E3 owner принимает только точные selections, без отдельного режима истории."""

from datetime import datetime, timezone

import pytest

from ci_test.test_demand_daily_preparation import config, module

requests = module("restored", "requests")
NOW = datetime(2026, 9, 9, tzinfo=timezone.utc)


def conf():
    return {
        "selections": [
            {
                "run_id": "e3-exact",
                "prediction_date": "2026-09-08",
                "start": "2022-09-01",
                "end": "2022-09-03",
            },
            {
                "run_id": "e3-exact",
                "prediction_date": "2026-09-08",
                "start": "2026-09-07",
                "end": "2026-09-08",
            },
        ]
    }


def test_request_is_stable_and_preserves_disjoint_ranges():
    request = requests.prepare_owner_request(config("restored"), conf(), run_id="copy", run_after=NOW)
    assert request == requests.prepare_owner_request(
        config("restored"), conf(), run_id="copy", run_after=NOW
    )
    assert request["dates"] == ["2022-09-01", "2022-09-02", "2026-09-07"]


def test_cutoff_day_uses_tashkent_not_utc_midnight():
    now = datetime(2026, 9, 7, 20, tzinfo=timezone.utc)
    request = requests.prepare_owner_request(config("restored"), conf(), run_id="copy", run_after=now)
    assert request["dates"][-1] == "2026-09-07"
    with pytest.raises(ValueError, match="будущем"):
        requests.prepare_owner_request(
            config("restored"), conf(), run_id="copy", run_after=now.replace(hour=18)
        )


@pytest.mark.parametrize("failure", ["empty", "latest", "future", "unknown", "mixed", "naive", "mode"])
def test_bad_requests_fail_before_connections(failure):
    value, now = conf(), NOW
    if failure == "empty":
        value = {}
    elif failure == "latest":
        value["selections"][0]["run_id"] = "latest"
    elif failure == "future":
        value["selections"][0]["prediction_date"] = "2027-01-01"
    elif failure == "unknown":
        value["force"] = True
    elif failure == "mixed":
        value["selections"][0]["run_id"] = "another"
    elif failure == "naive":
        now = NOW.replace(tzinfo=None)
    else:
        value["mode"] = "manual"
    with pytest.raises(ValueError):
        requests.prepare_owner_request(config("restored"), value, run_id="copy", run_after=now)
