"""Общий бюджет owner не возобновляется на следующей задаче или попытке."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from importlib import import_module
import sys
from types import ModuleType, SimpleNamespace

import pytest

from ci_test.test_demand_range_requests import PATHS as RANGE_PATHS
from dq.task import guarded_task

START = datetime(2026, 9, 9, 4, tzinfo=timezone.utc)
PATHS = [*RANGE_PATHS,
    "layers/silver/sku_id/demand_sales_daily/v1",
    "layers/silver/seller_id/demand_catalog_seller/v1",
    "layers/silver/sku_id/demand_catalog_sku/v1",
    "layers/silver/level_node_id/demand_catalog_tree/v1",
    "layers/gold/sku_id/demand_observed_daily/v1",
]


@pytest.fixture(params=PATHS)
def budget(request):
    return import_module(request.param.replace("/", ".") + ".job.budget")


def config():
    # Синтетические лимиты; не значения поставки.
    return {"runtime": {"run_timeout_seconds": {"regular": 600, "manual": 3600}}}


def context(mode="regular", started=START):
    return {"dag_run": SimpleNamespace(start_date=started, conf={"mode": mode})}


@pytest.mark.parametrize("mode,expected", [("regular", 500), ("manual", 3500)])
def test_fixed_budget_uses_dag_start_not_task_try(budget, mode, expected):
    ctx = context(mode)
    assert budget.remaining_seconds(config(), ctx, now=START + timedelta(seconds=100)) == expected
    ctx["try_number"] = 2
    assert budget.remaining_seconds(config(), ctx, now=START + timedelta(seconds=200)) == expected - 100


@pytest.mark.parametrize("elapsed", [600, 601])
def test_exhausted_budget_fails_closed(budget, elapsed):
    with pytest.raises(TimeoutError):
        budget.remaining_seconds(config(), context(), now=START + timedelta(seconds=elapsed))


@pytest.mark.parametrize("limits", [None, {}, {"regular": 0, "manual": 20},
    {"regular": True, "manual": 20}, {"regular": 20.0, "manual": 30},
    {"regular": 20, "manual": 10}, {"regular": 10, "manual": 20, "other": 30}])
def test_bad_config_rejected(budget, limits):
    with pytest.raises(ValueError):
        budget.configured_limits({"runtime": {"run_timeout_seconds": limits}})


@pytest.mark.parametrize("started", [None, START.replace(tzinfo=None), START + timedelta(seconds=1)])
def test_invalid_start_cannot_reset_budget(budget, started):
    with pytest.raises(ValueError):
        budget.remaining_seconds(config(), context(started=started), now=START)


def test_unknown_mode_rejected(budget):
    with pytest.raises(ValueError):
        budget.remaining_seconds(config(), context("other"), now=START)


def test_guard_covers_connections_body_and_exit(monkeypatch):
    events = []
    sdk = ModuleType("airflow.sdk")
    ctx = context()
    sdk.get_current_context = lambda: ctx
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    @contextmanager
    def guard(actual):
        assert actual is ctx
        events.append("budget")
        try:
            yield
        finally:
            events.append("closed")
    @guarded_task(guard)
    def task(value):
        events.append("source")
        raise RuntimeError(value)
    with pytest.raises(RuntimeError, match="failed"):
        task("failed")
    assert events == ["budget", "source", "closed"]


def test_expired_guard_cannot_reach_source(monkeypatch):
    sdk = ModuleType("airflow.sdk")
    sdk.get_current_context = lambda: context()
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    @contextmanager
    def expired(_context):
        raise TimeoutError("expired")
        yield
    @guarded_task(expired)
    def task():
        pytest.fail("Источник не должен открываться")
    with pytest.raises(TimeoutError, match="expired"):
        task()


def test_legacy_task_is_not_wrapped():
    def task(value):
        return value
    assert guarded_task(None)(task) is task


def test_restored_owner_uses_one_scalar_budget():
    budget = import_module(
        "layers.silver.sku_id_estimate_kind.demand_restored_daily.v1.job.budget"
    )
    cfg = {"runtime": {"run_timeout_seconds": 600}}
    assert budget.remaining_seconds(cfg, context(), now=START + timedelta(seconds=100)) == 500
