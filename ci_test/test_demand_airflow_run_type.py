"""Тип запуска Task SDK — str/Enum без переопределения __str__, не core Enum."""

import ast
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

from ci_test.test_demand_catalog_dags import PATHS as CATALOG_PATHS
from ci_test.test_demand_monthly_backfill import ENTITIES

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 10, 12, tzinfo=timezone.utc)
DAILY_PATHS = [f"layers/{entity}/v1" for entity in ENTITIES]
CALENDAR_PATHS = [
    "layers/silver/date/demand_calendar/v1",
    "layers/silver/event_code/demand_event_calendar/v1",
    "layers/gold/date/demand_calendar_daily/v1",
]


class DagRunType(str, Enum):
    # Та же семантика, что в airflow.sdk.api.datamodels._generated Airflow 3.1.8.
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    BACKFILL = "backfill"
    ASSET_TRIGGERED = "asset_triggered"


@pytest.fixture(params=[str, DagRunType], ids=["string", "task-sdk-enum"])
def run_type(request):
    return request.param


def config(path):
    return yaml.safe_load((ROOT / path / "config.yaml").read_text())


def job(path, name):
    return import_module(path.replace("/", ".") + ".job." + name)


def exact(source):
    return {"dag_id": source["dag"]["id"], "run_id": "manual-exact-source",
            "logical_date": NOW.isoformat()}


def inputs(cfg):
    return {key.removesuffix("_config"): yaml.safe_load((ROOT / value).read_text())
            for key, value in cfg.get("inputs", {}).items() if key.endswith("_config")}


def daily(path):
    cfg = config(path)
    name = "seller_requests" if "/demand_sales_daily/" in path else "requests"
    module = job(path, name)
    refs = {key: exact(source) for key, source in inputs(cfg).items()}
    if name == "seller_requests":
        kwargs = {"reference": next(iter(refs.values()))}
    elif "/gold/" in path:
        kwargs = {"references": refs}
    else:
        kwargs = {}
    return cfg, module, kwargs


def interval(cfg):
    minute, hour, *_ = cfg["dag"]["schedule"].split()
    end = NOW.replace(hour=int(hour), minute=int(minute))
    start = end - timedelta(days=1)
    return {"run_id": "scheduled__" + end.isoformat(), "logical_date": start,
            "interval_start": start, "interval_end": end}


def test_fixture_reproduces_sdk_string_conversion():
    assert str(DagRunType.SCHEDULED) == "DagRunType.SCHEDULED"
    assert DagRunType.SCHEDULED.value == "scheduled"


@pytest.mark.parametrize("path", DAILY_PATHS)
@pytest.mark.parametrize("kind", ["scheduled", "manual"])
def test_daily_window_and_budget_follow_actual_run_type(path, kind, run_type):
    cfg, module, refs = daily(path)
    args = interval(cfg) | {"logical_date": NOW if kind == "manual" else interval(cfg)["logical_date"]}
    result = module.owner_arguments(cfg, {}, run_type=run_type(kind), run_after=NOW, **args, **refs)
    assert result == module.owner_arguments(cfg, {}, run_type=kind, run_after=NOW, **args, **refs)
    assert result["mode"] == ("manual" if kind == "manual" else "regular")
    if kind == "manual":
        assert result["history_start"] == date(2026, 8, 10)
        assert result["interval_end"] == "2026-09-10T00:00:00+00:00"
    run = SimpleNamespace(conf={}, run_type=run_type(kind), start_date=NOW)
    budget = job(path, "budget").remaining_seconds(cfg, {"dag_run": run}, now=NOW + timedelta(seconds=10))
    assert budget == cfg["runtime"]["run_timeout_seconds"][result["mode"]] - 10


@pytest.mark.parametrize("path", DAILY_PATHS)
@pytest.mark.parametrize("kind", ["scheduled", "manual", "backfill", "asset_triggered"])
def test_daily_rejects_wrong_mode_or_unsupported_type(path, kind, run_type):
    cfg, module, refs = daily(path)
    conf = {"mode": "manual" if kind == "scheduled" else "regular"}
    with pytest.raises(ValueError):
        module.owner_arguments(cfg, conf, run_type=run_type(kind), run_after=NOW, **interval(cfg), **refs)


@pytest.mark.parametrize("path", CATALOG_PATHS + [DAILY_PATHS[3], DAILY_PATHS[4]])
@pytest.mark.parametrize("kind", ["scheduled", "manual", "backfill", "asset_triggered"])
def test_catalog_and_daily_upstream_references(path, kind, run_type):
    cfg = config(path)
    sources = inputs(cfg)
    conf = {"mode": "manual"} if kind == "manual" else {}
    if "/demand_catalog_" in path:
        source = next(iter(sources.values()), None)
        if kind == "manual" and source:
            conf["reference"] = exact(source)
        def call(value):
            return job(path, "requests").capture_request(cfg, source, conf, run_type=value, **interval(cfg))
    elif "/gold/" in path:
        if kind == "manual":
            conf["references"] = {key: exact(src) for key, src in sources.items()}
        def call(value):
            return job(path, "requests").resolve_references(cfg, sources, conf, run_type=value, **interval(cfg))
    else:
        source = next(iter(sources.values()))
        if kind == "manual":
            conf["reference"] = exact(source)
        def call(value):
            return job(path, "seller_requests").resolve_reference(cfg, source, conf, run_type=value, **interval(cfg))
    if kind in {"backfill", "asset_triggered"}:
        with pytest.raises(ValueError):
            call(run_type(kind))
    else:
        assert call(run_type(kind)) == call(kind)


def calendar_task(path, context):
    # Исполняется неизменённое тело реальной task; только декоратор Airflow снят.
    tree = ast.parse((ROOT / path / "dag.py").read_text())
    name = "write_calendar" if path == CALENDAR_PATHS[0] else "prepare_reference"
    task = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)
    task.decorator_list = []
    cfg = config(path)
    sources = inputs(cfg)
    scope = {"CONFIG": cfg, "REPO_ROOT": str(ROOT), "SOURCES": sources,
             "CALENDAR_CONFIG": sources.get("calendar"), "get_current_context": lambda: context}
    exec(compile(ast.Module(body=[task], type_ignores=[]), str(ROOT / path / "dag.py"), "exec"), scope)
    return scope[name]


@pytest.mark.parametrize("path", CALENDAR_PATHS)
@pytest.mark.parametrize("kind", ["scheduled", "manual"])
def test_calendar_tasks_accept_sdk_enum_and_preserve_references(path, kind, run_type, monkeypatch):
    cfg = config(path)
    bounds = interval(cfg)
    context = {"dag_run": SimpleNamespace(conf={}, run_type=run_type(kind)), "run_id": bounds["run_id"],
               "data_interval_start": bounds["interval_start"], "data_interval_end": bounds["interval_end"]}
    if kind == "manual":
        context["dag_run"].conf = {"mode": "manual"}
        sources = inputs(cfg)
        if path == CALENDAR_PATHS[1]:
            context["dag_run"].conf["calendar_reference"] = exact(sources["calendar"])
        elif path == CALENDAR_PATHS[2]:
            context["dag_run"].conf["references"] = {key: exact(src) for key, src in sources.items()}
    loader = Mock(return_value={"status": "written"})
    monkeypatch.setattr(job(path, "runtime"), "execute_load", loader)
    task = calendar_task(path, context)
    result = task()
    context["dag_run"].run_type = kind
    assert result == task()
    if path == CALENDAR_PATHS[0]:
        assert loader.call_args.args[-1] == ("manual" if kind == "manual" else "regular")
    else:
        loader.assert_not_called()
        assert result["mode"] == ("manual" if kind == "manual" else "regular")
        references = [result["reference"]] if "reference" in result else result["references"].values()
        for ref in references:
            assert ref["run_id"] == ("manual-exact-source" if kind == "manual" else
                                     "scheduled__" + (datetime.fromisoformat(ref["logical_date"]) + timedelta(days=1)).isoformat())


@pytest.mark.parametrize("path", CALENDAR_PATHS)
@pytest.mark.parametrize("kind", ["scheduled", "manual", "backfill", "asset_triggered"])
def test_calendar_rejects_wrong_mode_or_unsupported_type_before_io(path, kind, run_type, monkeypatch):
    runtime = job(path, "runtime")
    loader = Mock(side_effect=AssertionError("Не открывать IO"))
    monkeypatch.setattr(runtime, "execute_load", loader)
    mode = "manual" if kind == "scheduled" else "regular"
    context = {"dag_run": SimpleNamespace(conf={"mode": mode}, run_type=run_type(kind)), "run_id": "test"}
    with pytest.raises(ValueError):
        calendar_task(path, context)()
    loader.assert_not_called()
