"""Единый owner: scheduled-окно 31 день и явный ручной диапазон."""

from datetime import date, datetime, timezone
from importlib import import_module
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PATHS = [
    "layers/silver/sku_id_seller_key/demand_seller_sales_observed_daily/v1",
    "layers/silver/sku_id/demand_stock_daily/v1",
    "layers/silver/sku_id_seller_key/demand_finance_daily/v1",
]
NOW = datetime(2026, 9, 9, 4, tzinfo=timezone.utc)


@pytest.fixture(params=PATHS)
def entity(request):
    path = request.param
    return (
        yaml.safe_load((ROOT / path / "config.yaml").read_text()),
        import_module(path.replace("/", ".") + ".job.requests"),
        import_module(path.replace("/", ".") + ".job.orchestration"),
    )


def arguments(entity, conf=None, **kwargs):
    cfg, module, _ = entity
    params = {
        "run_id": "owner-run",
        "run_type": "scheduled",
        "interval_start": "2026-09-08T04:00:00Z",
        "interval_end": "2026-09-09T04:00:00Z",
        "run_after": NOW,
    }
    return module.owner_arguments(cfg, conf, **(params | kwargs))


def test_scheduled_uses_configured_floor_and_31_day_refresh(entity):
    result = arguments(entity)
    assert result["mode"] == "regular"
    assert result["history_start"] == date(2022, 9, 1)
    assert entity[0]["runtime"]["refresh_days"] == 31


def test_manual_uses_explicit_first_to_first_range(entity):
    result = arguments(
        entity,
        {"mode": "manual", "start": "2026-08-01", "end": "2026-09-01"},
        run_type="manual",
        interval_start=None,
        interval_end=None,
    )
    assert result["history_start"] == date(2026, 8, 1)
    assert result["interval_start"] == "2026-08-01T00:00:00+00:00"
    assert result["interval_end"] == "2026-09-01T00:00:00+00:00"


def test_manual_prepares_all_requested_days_without_coverage_read(entity):
    cfg, module, _ = entity

    def forbidden(*_args, **_kwargs):
        pytest.fail("Ручной диапазон не должен читать DQ-покрытие")

    result = module.prepare_owner_request(
        cfg,
        ROOT,
        {"mode": "manual", "start": "2026-08-01", "end": "2026-09-01"},
        run_id="owner",
        run_type="manual",
        interval_start=None,
        interval_end=None,
        run_after=NOW,
        catalog=forbidden,
        query=forbidden,
    )
    assert result["dates"][0] == "2026-08-01"
    assert result["dates"][-1] == "2026-08-31"
    assert len(result["dates"]) == 31


@pytest.mark.parametrize(
    "conf,run_type",
    [
        ({"mode": "manual", "start": "2026-08-01"}, "manual"),
        ({"mode": "manual", "end": "2026-09-01"}, "manual"),
        ({"mode": "manual", "start": "2026-09-01", "end": "2026-09-01"}, "manual"),
        ({"mode": "manual", "start": "2026-08-01", "end": "2026-09-10"}, "manual"),
        ({"mode": "regular"}, "manual"),
        ({"mode": "manual", "start": "2026-08-01", "end": "2026-09-01"}, "scheduled"),
        ({"skip_dq": True}, "scheduled"),
    ],
)
def test_invalid_contract_is_rejected_before_io(entity, conf, run_type, monkeypatch):
    cfg, module, bridge = entity
    monkeypatch.setattr(bridge, "prepare_request", lambda *_a, **_k: pytest.fail("Не открывать IO"))
    with pytest.raises(ValueError):
        module.prepare_owner_request(
            cfg,
            ROOT,
            conf,
            run_id="run",
            run_type=run_type,
            interval_start="2026-09-08T04:00:00Z",
            interval_end="2026-09-09T04:00:00Z",
            run_after=NOW,
        )
