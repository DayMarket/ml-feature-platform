"""Проверить реальные Airflow DAG/API локально в согласованном image, без сервисов."""

from importlib import import_module
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("airflow", reason="Нужен согласованный Airflow image")
import pendulum
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.base import DataInterval
from airflow.utils.types import DagRunType

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture
def namespace():
    return runpy.run_path(str(ROOT / "layers/silver/event_code/demand_event_calendar/v1/dag.py"))


def test_actual_dag_graph_resources_and_timetable(namespace):
    dag = namespace["dag"]
    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.max_active_runs == 1 and not dag.catchup and dag.is_paused_upon_creation
    assert set(dag.task_ids) == {"prepare_reference", "wait_for_calendar_dq", "write_events", "dq", "feature_stats"}
    assert dag.get_task("write_events").downstream_task_ids == {"dq", "feature_stats"}
    for key in ("dq", "feature_stats"):
        assert not dag.get_task(key).downstream_task_ids
    sensor = dag.get_task("wait_for_calendar_dq")
    assert sensor.external_task_id == "dq" and sensor.mode == "reschedule"
    for task in dag.tasks:
        pod = task.executor_config["pod_override"].spec.containers[0]
        assert pod.resources.requests == {"cpu": "1", "memory": "2Gi"}


def test_scheduled_reference_matches_airflow_timetable(namespace):
    runtime = import_module("layers.silver.event_code.demand_event_calendar.v1.job.runtime")
    start, end = pendulum.parse("2026-09-08T03:10:00Z"), pendulum.parse("2026-09-09T03:10:00Z")
    ref = runtime.scheduled_calendar_reference(namespace["CONFIG"], namespace["CALENDAR_CONFIG"], start, end)
    calendar = runpy.run_path(str(ROOT / "layers/silver/date/demand_calendar/v1/dag.py"))["dag"]
    assert isinstance(calendar.timetable, CronDataIntervalTimetable)
    expected = calendar.timetable.generate_run_id(run_type=DagRunType.SCHEDULED,
                                                  run_after=end.subtract(minutes=10),
                                                  data_interval=DataInterval(start.subtract(minutes=10), end.subtract(minutes=10)))
    assert ref["run_id"] == expected
    ti = Mock()
    ti.xcom_pull.return_value = {"reference": ref}
    assert namespace["calendar_logical_date"](None, ti=ti) == pendulum.parse(ref["logical_date"])


def test_writer_reads_exact_dq_run_no_prior_dates(namespace, monkeypatch):
    runtime = import_module("layers.silver.event_code.demand_event_calendar.v1.job.runtime")
    load = Mock(return_value={"status": "written"})
    monkeypatch.setattr(runtime, "execute_load", load)
    ti = Mock()
    checked = {"dq_status": "passed"}
    ti.xcom_pull.return_value = checked
    fn = namespace["dag"].get_task("write_events").python_callable
    monkeypatch.setitem(fn.__globals__, "get_current_context", lambda: {"ti": ti, "run_id": "event-run"})
    reference = {"dag_id": "calendar-owner", "run_id": "exact-run", "logical_date": "2026-09-08T03:00:00Z"}
    fn({"mode": "regular", "reference": reference})
    ti.xcom_pull.assert_called_once_with(dag_id="calendar-owner", task_ids="dq", run_id="exact-run", include_prior_dates=False)
    assert load.call_args.args[-1] is checked


@pytest.mark.parametrize("failure", [None, "dq", "results_write"])
def test_checked_receipt_only_after_successful_dq_and_results(monkeypatch, failure):
    import airflow.sdk
    import airflow.providers.trino.hooks.trino

    module = import_module("dq.task")
    result = {"status": "written", "snapshot_id": 123, "rows_written": 3, "table_uuid": "uuid",
              "source_manifest_id": "run-1", "ingested_at": "2026-09-08T03:10:00+00:00"}
    ti = Mock(dag_id="owner", run_id="run-1", try_number=1)
    ti.xcom_pull.return_value = result
    monkeypatch.setattr(airflow.sdk, "get_current_context", lambda: {"task_instance": ti})
    monkeypatch.setattr(airflow.providers.trino.hooks.trino, "TrinoHook", Mock())
    monkeypatch.setattr(module, "run_dq", Mock(return_value=SimpleNamespace(has_errors=failure == "dq")))
    monkeypatch.setattr(module, "format_log", Mock(return_value="checked"))
    monkeypatch.setattr(module, "format_alert", Mock(return_value="failed"))
    saved = Mock(side_effect=RuntimeError("results write failed") if failure == "results_write" else None)
    monkeypatch.setattr(module, "write_results", saved)
    ns = runpy.run_path(str(ROOT / "layers/silver/event_code/demand_event_calendar/v1/dag.py"))
    task = ns["dag"].get_task("dq")
    assert not task.multiple_outputs
    if failure:
        with pytest.raises((RuntimeError, module.DqTestsFailed)):
            task.python_callable(result["ingested_at"])
    else:
        assert task.python_callable(result["ingested_at"]) == {
            "dq_status": "passed", "dag_id": "owner", "run_id": "run-1", "receipt": result}
    saved.assert_called_once()
