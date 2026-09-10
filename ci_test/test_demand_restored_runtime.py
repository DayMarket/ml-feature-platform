"""Проверить точный E3-run/hold и fail-closed streaming без внешних подключений."""

from datetime import timedelta

import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, module
from ci_test.test_demand_restored_writer import bundle, target, write  # noqa: F401
from ci_test.test_demand_sales_finance_runtime import wire


class Client:
    def __init__(self, cfg, data):
        self.config, self.data = cfg, data
        self.passport_reads, self.streamed, self.closed = 0, False, False
        self.failure = None

    def execute(self, sql, *, params, with_column_types):
        runtime = module("restored", "runtime")
        assert sql == module("restored", "query").registry_query(self.config)
        assert params == self.data[0] and with_column_types is True
        self.passport_reads += 1
        passport = dict(self.data[1])
        if self.failure == "status":
            passport["status"] = "written"
        if self.failure == "changed" and self.passport_reads > 1:
            passport["state_version"] += 1
        columns = [(name, "unused_metadata_type") for name in runtime.REGISTRY_COLUMNS]
        return [[passport[name] for name, _ in columns]], columns

    def execute_iter(self, sql, *, params, **kwargs):
        assert sql == module("restored", "query").source_query(self.config)
        assert params["run_id"] == self.data[0]["run_id"]
        assert params["start"] == DAY and params["end"] == DAY + timedelta(days=1)
        chunk_size = self.config["runtime"]["max_batch_rows"]
        assert kwargs["chunk_size"] == chunk_size and kwargs["with_column_types"] is True
        self.streamed = True
        try:
            rows_and_columns = [wire(batch, "restored") for batch in self.data[2]]
            columns = rows_and_columns[0][1]
            buffer = [columns]
            for index, (rows, _) in enumerate(rows_and_columns):
                if self.failure == "stream" and index == 1:
                    raise RuntimeError("connection lost")
                for row in rows:
                    if self.failure == "payload":
                        index = [n for n, _ in columns].index("lost_units")
                        row[index] = 9.0
                    buffer.append(row)
                    if len(buffer) == chunk_size:
                        yield buffer
                        buffer = []
            if buffer:
                yield buffer
        finally:
            self.closed = True


@pytest.fixture
def loader(target, monkeypatch):  # noqa: F811
    cfg, _, _ = target
    # chunk_size=1 у clickhouse-driver означает поэлементный iterator.
    cfg["runtime"]["max_batch_rows"] = 2
    data = bundle(kinds=("final", "provisional"))
    client = Client(cfg, data)
    monkeypatch.setattr(module("restored", "runtime"), "utc_now", lambda: CAPTURE + timedelta(seconds=1))
    return target, client


def load(loader, held=lambda selected, run: True):
    destination, client = loader
    return module("restored", "runtime").load_day(
        destination[0], destination[1], client, selected=client.data[0], day=DAY,
        manifest="copy-1", require_run_held=held)


def test_exact_run_loader_verifies_passport_twice_and_copies_both_kinds(loader):
    destination, client = loader
    receipt = load(loader)
    assert client.passport_reads == 2 and client.closed
    assert receipt["status"] == "written" and receipt["rows_written"] == 2
    assert receipt["source_day"]["source_run_id"] == client.data[0]["run_id"]
    assert destination[2].refresh().scan().to_arrow().num_rows == 2


@pytest.mark.parametrize("failure", ["status", "changed", "stream", "payload"])
def test_loader_failure_never_replaces_previous_day(loader, failure):
    destination, client = loader
    old = write(destination, bundle())
    client.failure = failure
    with pytest.raises((ValueError, RuntimeError)):
        load(loader)
    assert destination[2].refresh().current_snapshot().snapshot_id == old["snapshot_id"]
    if client.streamed:
        assert client.closed


def test_no_hold_blocks_before_reading_result(loader):
    with pytest.raises(ValueError, match="удерживается"):
        load(loader, held=lambda selected, run: False)
    assert not loader[1].streamed
    assert loader[0][2].refresh().current_snapshot() is None


def test_day_loader_refuses_passport_other_than_range_pin(loader):
    destination, client = loader
    expected = client.data[1] | {"state_version": client.data[1]["state_version"] + 1}
    with pytest.raises(ValueError, match="после подготовки диапазона"):
        module("restored", "runtime").load_day(
            destination[0], destination[1], client, selected=client.data[0], day=DAY,
            manifest="copy-1", require_run_held=lambda *args: True, expected_run=expected)
    assert not client.streamed


def test_hold_lost_before_commit_blocks(loader):
    calls = []
    def held(selected, run):
        calls.append((selected["run_id"], run["state_version"]))
        return len(calls) == 1
    with pytest.raises(ValueError, match="hold"):
        load(loader, held=held)
    assert len(calls) == 2 and loader[1].closed
    assert loader[0][2].refresh().current_snapshot() is None


def test_source_run_id_is_bound_as_parameter_not_interpolated(loader):
    destination, client = loader
    client.data = bundle(kinds=("final",), run_id="run-with-quote-'--")
    receipt = load(loader)
    assert receipt["source_day"]["source_run_id"] == "run-with-quote-'--"
    assert destination[2].refresh().scan().to_arrow()["run_id"].to_pylist() == ["run-with-quote-'--"]


def test_missing_target_fails_before_any_ch_query(loader):
    loader[0][0]["table"]["name"] = "missing"
    with pytest.raises(ValueError, match="миграции"):
        load(loader)
    assert loader[1].passport_reads == 0 and not loader[1].streamed


def test_missing_day_manifest_not_replaced_by_actual_source_count(loader):
    selected, passport, rows = loader[1].data
    passport["output_manifest"] = '{"outputs":1}'
    with pytest.raises(ValueError, match="fp_daily_copy"):
        load(loader)
    assert not loader[1].streamed
