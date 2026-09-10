"""Одна feature_stats task профилирует все дни записанного диапазона."""

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from ci_test.test_dq_day_range import Catalog, inputs
from feature_stats import day_range
from feature_stats.config import FeatureStatsConfigError
from feature_stats.results_writer import RunMeta
from feature_stats.runner import FeatureStatsError

ROOT = Path(__file__).resolve().parents[1]
META = RunMeta("dag-exact", "feature_stats", "run-exact", 1, datetime(2026, 9, 9, tzinfo=timezone.utc))
PROFILE = [2, 1, 2.0, 2.0, 2.0, [2.0] * 7]


def configure():
    config, written = inputs()
    config["feature_stats"] = {
        "partition_column": "date",
        "partition_date_template": "{{ ds }}",
    }
    config["dq"]["partition_date_template"] = "{{ ds }}"
    return config, written


def test_each_day_is_profiled_while_current_snapshot_is_unchanged(monkeypatch):
    config, written = configure()
    saved, queries = [], []
    monkeypatch.setattr(day_range, "write_results", lambda *args: saved.append(args))

    def query(sql):
        queries.append(sql)
        if "information_schema.columns" in sql:
            return [("date", "date"), ("sku_id", "bigint"), ("units", "double")]
        return [deepcopy(PROFILE)]

    assert day_range.run_range_feature_stats(
        config, ROOT, written, query, META, catalog=Catalog()
    ) is None
    assert len(saved) == 2
    assert all("FOR VERSION AS OF" not in sql for sql in queries)
    assert all(args[1][0].rows_total == 2 for args in saved)


@pytest.mark.parametrize(
    "columns,profile",
    [
        ([("date", "date"), ("units", "double")], []),
        ([("date", "date"), ("units", "double")], [PROFILE, PROFILE]),
        ([("date", "date"), ("units", "double")], [[3, *PROFILE[1:]]]),
        ([("date", "date"), ("units", "double")], [[2, 3, *PROFILE[2:]]]),
        ([("units", None)], [PROFILE]),
        ([("units", "double"), ("units", "double")], [PROFILE]),
        ([("date",)], [PROFILE]),
    ],
)
def test_malformed_profile_or_schema_blocks(monkeypatch, columns, profile):
    config, written = configure()
    monkeypatch.setattr(day_range, "write_results", lambda *args: pytest.fail("Не сохранять"))

    def query(sql):
        return columns if "information_schema.columns" in sql else profile

    with pytest.raises(FeatureStatsError):
        day_range.run_range_feature_stats(config, ROOT, written, query, META, catalog=Catalog())


def test_snapshot_change_blocks(monkeypatch):
    config, written = configure()
    catalog = Catalog()

    def query(sql):
        if "information_schema.columns" in sql:
            return [("date", "date"), ("units", "double")]
        catalog.head = 124
        return [deepcopy(PROFILE)]

    monkeypatch.setattr(day_range, "write_results", lambda *args: None)
    with pytest.raises(Exception, match="snapshot"):
        day_range.run_range_feature_stats(config, ROOT, written, query, META, catalog=catalog)


def test_range_settings_must_match_dq():
    config, _ = configure()
    config["feature_stats"]["partition_column"] = "other"
    with pytest.raises(FeatureStatsConfigError):
        day_range.validate_range_settings(config)


def test_factory_reads_writer_and_uses_range_timeout(monkeypatch):
    from feature_stats.task import build_feature_stats_task

    config, written = configure()
    config["alerts"] = {
        "team": "operations-analytics",
        "oncall_webhook_conn_id": "oncall_webhook_operations",
        "severity": "P3",
    }
    calls, options = [], []

    class TaskInstance:
        dag_id, run_id, try_number = META.dag_id, META.run_id, 1

        def xcom_pull(self, **kwargs):
            calls.append(kwargs)
            return written

    def fake_module(name, **attributes):
        module = ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, module)

    def decorator(**kwargs):
        options.append(kwargs)
        return lambda function: function

    fake_module("airflow.providers.trino.hooks.trino", TrinoHook=lambda **kwargs: SimpleNamespace())
    fake_module(
        "airflow.sdk",
        get_current_context=lambda: {"task_instance": TaskInstance()},
        task=decorator,
    )
    fake_module(
        "airflow_commons.helpers.oncall",
        send_oncall_notification=lambda **kwargs: None,
    )
    monkeypatch.setattr("feature_stats.task.yaml.safe_load", lambda value: config)
    monkeypatch.setattr(day_range, "run_range_feature_stats", lambda *args: None)
    path = ROOT / "layers/silver/sku_id/demand_stock_daily/v1/config.yaml"
    task = build_feature_stats_task(
        str(path), str(ROOT), range_receipt_task_id="write_range", range_timeout_seconds=1200
    )
    assert options[-1]["execution_timeout"].total_seconds() == 1200
    assert task(written["dates"][-1]) is None
    assert calls == [{"task_ids": "write_range", "include_prior_dates": False}]


@pytest.mark.parametrize("timeout", [None, 0, -1, True])
def test_factory_requires_positive_range_timeout(monkeypatch, timeout):
    from feature_stats.task import build_feature_stats_task

    config, _ = configure()
    config["alerts"] = {
        "team": "operations-analytics",
        "oncall_webhook_conn_id": "oncall_webhook_operations",
        "severity": "P3",
    }
    monkeypatch.setattr("feature_stats.task.yaml.safe_load", lambda value: config)
    for name, attributes in (
        ("airflow.providers.trino.hooks.trino", {"TrinoHook": lambda **kwargs: None}),
        ("airflow.sdk", {"get_current_context": lambda: {}, "task": lambda **kwargs: lambda fn: fn}),
        ("airflow_commons.helpers.oncall", {"send_oncall_notification": lambda **kwargs: None}),
    ):
        module = ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, module)
    path = ROOT / "layers/silver/sku_id/demand_stock_daily/v1/config.yaml"
    with pytest.raises(FeatureStatsConfigError):
        build_feature_stats_task(
            str(path),
            str(ROOT),
            range_receipt_task_id="write_range",
            range_timeout_seconds=timeout,
        )
