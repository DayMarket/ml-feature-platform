"""Узкий opt-in DQ нескольких дневных партиций одной terminal task."""

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from dq import day_range
from dq.config import DqConfigError
from dq.results_writer import RunMeta
from dq.runner import DqPreflightError
from dq.task import DqTestsFailed, build_render_context

ROOT = Path(__file__).resolve().parents[1]
META = RunMeta("dag-exact", "dq", "run-exact", 1, datetime(2026, 9, 9, tzinfo=timezone.utc))


class Catalog:
    name = "iceberg"

    def __init__(self):
        self.head = 123

    def table_exists(self, identifier):
        return identifier == ("silver", "feature_platform_test")

    def load_table(self, identifier):
        assert identifier == ("silver", "feature_platform_test")
        return SimpleNamespace(
            metadata=SimpleNamespace(table_uuid="table-uuid"),
            current_snapshot=lambda: SimpleNamespace(snapshot_id=self.head),
        )


def inputs():
    config = {
        "table": {
            "catalog": "iceberg",
            "schema": "silver",
            "name": "feature_platform_test",
            "primary_key": "date,sku_id",
        },
        "dq": {
            "warmup_days": 0,
            "tests": [
                {"name": "row_count_growth", "enabled": False},
                {"name": "freshness", "enabled": False},
            ],
        },
    }
    dates = ["2026-09-06", "2026-09-07"]
    receipts = [
        {
            "status": "written",
            "date": value,
            "table_uuid": "table-uuid",
            "rows_written": 2,
            "source_manifest_id": f"copy:{value}",
            "source_contract_version": "v1",
            "ingested_at": "2026-09-09T04:00:00+00:00",
        }
        for value in dates
    ]
    written = {
        "status": "written",
        "request_id": "request-exact",
        "snapshot_id": 123,
        "table_uuid": "table-uuid",
        "dates": dates,
        "day_receipts": receipts,
    }
    return config, written


def test_each_day_is_checked_and_saved_on_final_snapshot(monkeypatch):
    config, written = inputs()
    catalog, saved, queries = Catalog(), [], []
    monkeypatch.setattr(day_range, "write_results", lambda *args: saved.append(args))

    def query(sql):
        queries.append(sql)
        if "information_schema.tables" in sql:
            return [(1,)]
        return [(0, 2.0)]

    checks = day_range.run_range_dq(config, ROOT, written, query, META, catalog=catalog)
    assert [item["date"] for item in checks] == written["dates"]
    assert len(saved) == 2
    assert all(item["dq_status"] == "passed" and item["snapshot_id"] == 123 for item in checks)
    assert all("FOR VERSION AS OF" not in sql for sql in queries)


@pytest.mark.parametrize("response", [[], [(None, 0)], [(False, 0)], [(0,)], [(0, 0), (0, 0)]])
def test_malformed_dq_response_never_passes(monkeypatch, response):
    config, written = inputs()
    monkeypatch.setattr(day_range, "write_results", lambda *args: None)

    def query(sql):
        return [(1,)] if "information_schema.tables" in sql else response

    with pytest.raises(DqPreflightError):
        day_range.run_range_dq(config, ROOT, written, query, META, catalog=Catalog())


def test_failed_test_is_saved_then_blocks(monkeypatch):
    config, written = inputs()
    saved = []
    monkeypatch.setattr(day_range, "write_results", lambda *args: saved.append(args))

    def query(sql):
        if "information_schema.tables" in sql:
            return [(1,)]
        return [(1, 1.0)] if "HAVING count(*) > 1" in sql else [(0, 2.0)]

    with pytest.raises(DqTestsFailed):
        day_range.run_range_dq(config, ROOT, written, query, META, catalog=Catalog())
    assert len(saved) == 1


def test_snapshot_change_blocks_result(monkeypatch):
    config, written = inputs()
    catalog = Catalog()

    def write(*args):
        catalog.head = 124

    monkeypatch.setattr(day_range, "write_results", write)

    def query(sql):
        return [(1,)] if "information_schema.tables" in sql else [(0, 2.0)]

    with pytest.raises(DqConfigError, match="snapshot"):
        day_range.run_range_dq(config, ROOT, written, query, META, catalog=catalog)


def test_receipt_and_settings_are_fail_closed():
    config, written = inputs()
    broken = deepcopy(written)
    broken["day_receipts"].pop()
    with pytest.raises(DqConfigError):
        day_range.validate_written(broken)
    config["dq"]["warmup_days"] = 1
    with pytest.raises(DqConfigError):
        day_range.validate_settings(day_range.load_dq_settings(config))


def test_factory_reads_exact_writer_xcom(monkeypatch):
    from dq.task import build_dq_task

    config, written = inputs()
    config["alerts"] = {
        "team": "operations-analytics",
        "oncall_webhook_conn_id": "oncall_webhook_operations",
        "severity": "P3",
    }
    calls = []

    class TaskInstance:
        dag_id, run_id, try_number = META.dag_id, META.run_id, 1

        def xcom_pull(self, **kwargs):
            calls.append(kwargs)
            return written

    def fake_module(name, **attributes):
        module = ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, module)

    fake_module(
        "airflow.providers.trino.hooks.trino",
        TrinoHook=lambda **kwargs: SimpleNamespace(get_records=lambda sql: []),
    )
    fake_module(
        "airflow.sdk",
        get_current_context=lambda: {"task_instance": TaskInstance()},
        task=lambda **kwargs: lambda function: function,
    )
    fake_module(
        "airflow_commons.helpers.oncall",
        send_oncall_notification=lambda **kwargs: None,
    )
    monkeypatch.setattr("dq.task.yaml.safe_load", lambda value: config)
    checks = [{"dq_status": "passed"}]
    monkeypatch.setattr(day_range, "run_range_dq", lambda *args: checks)
    path = ROOT / "layers/silver/sku_id/demand_stock_daily/v1/config.yaml"
    task = build_dq_task(str(path), str(ROOT), range_receipt_task_id="write_range")
    result = task(written["dates"][-1])
    assert calls == [{"task_ids": "write_range", "include_prior_dates": False}]
    assert result["receipt"] is written and result["day_checks"] is checks
    with pytest.raises(DqConfigError, match="последним днём"):
        task(written["dates"][0])


def test_write_match_sql_escapes_values_and_uses_utc_snapshot():
    config, written = inputs()
    receipt = deepcopy(written["day_receipts"][0])
    receipt["source_manifest_id"] = "quote'value"
    receipt["ingested_at"] = "2026-09-09T09:00:00+05:00"
    context = build_render_context(config, ROOT, receipt["date"])
    sql = day_range.write_matches_sql(context, receipt)
    assert "'quote''value'" in sql
    assert "TIMESTAMP '2026-09-09 04:00:00.000000 UTC'" in sql
    assert "FOR VERSION AS OF" not in sql


@pytest.mark.parametrize("growth_response", [(0, 0.01), (1, -0.95), (-1, None)])
def test_live_day_growth_blocks_bad_or_missing_baseline_but_history_can_start(monkeypatch, growth_response):
    config, written = inputs()
    config["dq"]["tests"] = []
    written["dates"] = ["2026-09-07", "2026-09-08"]
    for day, receipt in zip(written["dates"], written["day_receipts"]):
        receipt["date"] = day
    saved, growth_queries = [], []
    monkeypatch.setattr(day_range, "write_results", lambda *args: saved.append(args))

    def query(sql):
        if "information_schema.tables" in sql:
            return [(1,)]
        if "previous_row_count" in sql:
            growth_queries.append(sql)
            return [growth_response]
        return [(0, 2.0)]

    if growth_response[0] == 0:
        assert len(day_range.run_range_dq(config, ROOT, written, query, META, catalog=Catalog())) == 2
    else:
        with pytest.raises(DqTestsFailed):
            day_range.run_range_dq(config, ROOT, written, query, META, catalog=Catalog())
    assert len(growth_queries) == 1
    assert len(saved) == 2
    historical = {spec.name for spec in saved[0][3].tests}
    live = {spec.name for spec in saved[1][3].tests}
    assert not historical & {"freshness", "row_count_growth"}
    assert {"freshness", "row_count_growth"} <= live


def test_retry_keeps_live_day_checks_relative_to_writer_capture():
    config, written = inputs()
    config["dq"]["tests"] = []
    settings = day_range.load_dq_settings(config)
    receipt = written["day_receipts"][0]
    day = day_range.date(2026, 9, 8)
    assert day_range.settings_for_day(settings, day, receipt) is settings
