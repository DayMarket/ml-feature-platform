"""Ручное окно от logical_date покрывает историю месячными запусками."""

from datetime import date, datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITIES = [
    "silver/sku_id/demand_stock_daily",
    "silver/sku_id_seller_key/demand_finance_daily",
    "silver/sku_id_seller_key/demand_seller_sales_observed_daily",
    "silver/sku_id/demand_sales_daily",
    "gold/sku_id/demand_observed_daily",
]
NOW = datetime(2026, 9, 10, 12, tzinfo=timezone.utc)


def arguments(entity, logical_date, conf=None):
    config = yaml.safe_load((ROOT / "layers" / entity / "v1/config.yaml").read_text())
    name = "seller_requests" if entity.endswith("/demand_sales_daily") else "requests"
    module = import_module("layers." + entity.replace("/", ".") + f".v1.job.{name}")
    conf = dict(conf or {})
    reference = {"dag_id": "exact-source", "run_id": "exact-run", "logical_date": NOW.isoformat()}
    if name == "seller_requests":
        conf["reference"] = reference
    elif entity.startswith("gold/"):
        conf["references"] = {kind: reference for kind in ("sales", "stock")}
    return module.owner_arguments(
        config, conf, run_id="manual__history", run_type="manual", logical_date=logical_date,
        interval_start=NOW - timedelta(days=1), interval_end=NOW, run_after=NOW,
    )


@pytest.mark.parametrize("entity", ENTITIES)
def test_first_of_month_runs_plus_today_cover_all_completed_history(entity):
    ends = [date(year, month, 1) for year in range(2022, 2027) for month in range(1, 13)
            if date(2022, 9, 1) < date(year, month, 1) <= NOW.date()]
    ends.append(NOW.date())
    covered = set()
    for end in ends:
        args = arguments(entity, datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc))
        first = args["history_start"]
        assert args["mode"] == "manual"
        assert date.fromisoformat(args["interval_end"][:10]) == end
        assert first == max(date(2022, 9, 1), end - timedelta(days=31))
        covered.update(first + timedelta(days=i) for i in range((end - first).days))
    floor = date(2022, 9, 1)
    assert covered == {floor + timedelta(days=i) for i in range((NOW.date() - floor).days)}


@pytest.mark.parametrize("entity", ENTITIES)
def test_explicit_range_overrides_logical_date_and_allows_no_logical_date(entity):
    args = arguments(entity, None, {"start": "2026-08-01", "end": "2026-09-01"})
    assert args["history_start"] == date(2026, 8, 1)
    assert args["interval_end"] == "2026-09-01T00:00:00+00:00"


@pytest.mark.parametrize("entity", ENTITIES)
@pytest.mark.parametrize("conf", [{"start": "2026-08-01"}, {"end": "2026-09-01"}])
def test_partial_explicit_range_never_falls_back(entity, conf):
    with pytest.raises(ValueError, match="вместе"):
        arguments(entity, NOW, conf)


@pytest.mark.parametrize("entity", ENTITIES)
def test_manual_without_range_or_logical_date_is_rejected(entity):
    with pytest.raises(ValueError, match="logical_date"):
        arguments(entity, None)


@pytest.mark.parametrize("entity", ENTITIES)
def test_default_manual_uses_manual_run_budget(entity):
    config = yaml.safe_load((ROOT / "layers" / entity / "v1/config.yaml").read_text())
    budget = import_module("layers." + entity.replace("/", ".") + ".v1.job.budget")
    run = SimpleNamespace(conf={}, run_type="manual", start_date=NOW)
    assert budget.remaining_seconds(config, {"dag_run": run}, now=NOW) == config["runtime"]["run_timeout_seconds"]["manual"]
