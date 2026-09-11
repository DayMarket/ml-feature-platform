"""Проверить streaming CH → Arrow → Iceberg без обращений к внешним источникам."""

from datetime import timedelta
from decimal import Decimal

import pyarrow as pa
import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, config, fx, module, raw, schema
from ci_test.test_demand_sales_finance_writer import write


@pytest.fixture
def env(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog

    kind, cfg = "finance", config("finance")
    catalog = SqlCatalog(
        "iceberg",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    catalog.create_namespace("silver")
    table = catalog.create_table(
        module(kind, "preparation").target_ref(cfg, catalog.name), schema=schema(kind)
    )
    with table.update_spec() as spec:
        spec.add_identity("date")
    yield kind, cfg, catalog, table
    catalog.engine.dispose()


def wire(source, kind):
    columns = []
    rows = source.to_pylist()
    for field in source.schema:
        if pa.types.is_date(field.type):
            dtype = "Date"
        elif pa.types.is_integer(field.type):
            dtype = "Decimal(38, 0)" if kind == "finance" and field.name.startswith("finance_") else "Int64"
            if dtype.startswith("Decimal"):
                for row in rows:
                    row[field.name] = Decimal(row[field.name]) if row[field.name] is not None else None
        elif pa.types.is_decimal(field.type):
            dtype = "Decimal(38, 0)"
        elif pa.types.is_floating(field.type):
            dtype = "Float64"
        elif pa.types.is_timestamp(field.type):
            dtype = "DateTime64(6, 'UTC')"
        else:
            dtype = "String"
        if source[field.name].null_count:
            dtype = f"Nullable({dtype})"
        columns.append((field.name, dtype))
    return [[row[name] for name, _ in columns] for row in rows], columns


class Client:
    def __init__(self, kind, cfg, source):
        self.kind, self.config, self.source = kind, cfg, source
        self.queries, self.coverage_calls, self.fx_calls = [], 0, 0
        self.failure, self.closed, self.streamed = None, False, False
        self.fx = fx()

    def coverage(self):
        names = module(self.kind, "query").TOTAL_COLUMNS
        result = [self.source.num_rows]
        result += [sum(int(v) for v in self.source[name].to_pylist()) for name in names]
        if self.kind == "sales":
            result += [max(self.source["source_updated_at"].to_pylist())]
        return result

    def execute(self, sql):
        self.queries.append(sql)
        query = module(self.kind, "query")
        if sql == query.coverage_totals_query(self.config, DAY):
            self.coverage_calls += 1
            result = self.coverage()
            if self.failure == "source_changed" and self.coverage_calls > 1:
                result[1] += 1
            if self.failure == "empty":
                result[0] = 0
            return [result]
        if sql == query.fx_query(DAY):
            self.fx_calls += 1
            result = dict(self.fx)
            if self.failure == "fx_changed" and self.fx_calls > 1:
                result["fx_rate_uzs_per_usd"] *= 2
            return [list(result.values())]
        if self.kind == "finance" and sql == query.source_audit_query(self.config, DAY):
            return [[sum(self.source["source_rows"].to_pylist()), 0, int(self.failure == "audit")]]
        if self.kind == "sales" and sql == query.coverage_query(self.config, DAY):
            count = sum(self.source["sales_order_items"].to_pylist())
            return [[count, count + int(self.failure == "audit"), self.source.num_rows,
                     sum(self.source["sales_units"].to_pylist()),
                     Decimal(sum(int(v) for v in self.source["sales_gmv"].to_pylist()))]]
        raise AssertionError(sql)

    def execute_iter(self, sql, **kwargs):
        chunk_size = self.config["runtime"]["max_batch_rows"]
        assert kwargs["chunk_size"] == chunk_size and kwargs["with_column_types"]
        assert kwargs["settings"]["max_block_size"] == chunk_size
        assert sql == module(self.kind, "query").source_query(
            self.config, DAY, fx_available=self.fx["fx_rate_source"] != "unavailable")
        rows, columns = wire(self.source, self.kind)
        self.streamed = True
        try:
            if self.failure == "metadata":
                return
            buffer = [columns]
            for index, row in enumerate(rows):
                if self.failure == "interrupted" and index == 1:
                    raise RuntimeError("stream interrupted")
                if self.failure == "payload" and index == 0:
                    name = "sales_payment_value" if self.kind == "sales" else "finance_gmv_generated"
                    idx = [n for n, _ in columns].index(name)
                    row[idx] += 10
                    usd = [n for n, _ in columns].index(name + "_usd")
                    row[usd] += 1
                buffer.append(row)
                if len(buffer) == chunk_size:
                    yield buffer
                    buffer = []
            if buffer:
                yield buffer
        finally:
            self.closed = True


@pytest.fixture
def loader(env, monkeypatch):  # noqa: F811
    kind, cfg, catalog, table = env
    # Проверяем настоящую chunked-ветку clickhouse-driver; значение 1
    # отключает упаковку строк в порции.
    cfg["runtime"]["max_batch_rows"] = 2
    monkeypatch.setattr(module(kind, "runtime"), "utc_now", lambda: CAPTURE + timedelta(seconds=1))
    changes = {"sku_id": 2} if kind == "sales" else {"seller_id": None, "seller_key": "unknown"}
    source = pa.concat_tables([raw(kind), raw(kind, changes)])
    client = Client(kind, cfg, source)
    return env, client


def load(loader, *, ready=lambda day: True, manifest="capture-1"):
    (kind, cfg, catalog, _), client = loader
    return module(kind, "runtime").load_day(
        cfg, catalog, client, day=DAY, manifest=manifest, require_source_ready=ready)


def test_load_streams_each_key_and_copies_raw_money(loader):
    (kind, _, _, table), client = loader
    receipt = load(loader)
    assert receipt["status"] == "written" and "dq_status" not in receipt
    assert receipt["rows_written"] == 2 and client.closed
    data = table.refresh().scan().to_arrow()
    assert data.num_rows == 2
    amount = "sales_gmv" if kind == "sales" else "finance_gmv_net"
    assert sorted(data[amount].to_pylist()) == sorted(client.source[amount].to_pylist())
    assert client.coverage_calls == client.fx_calls == 2


def test_load_without_operational_statuses_still_checks_source(loader):
    (kind, cfg, catalog, _), client = loader
    receipt = module(kind, 'runtime').load_day(cfg, catalog, client, day=DAY, manifest='no-logs')
    assert receipt['rows_written'] == 2 and receipt['status'] == 'written'
    assert 'dq_status' not in receipt
    assert client.coverage_calls == client.fx_calls == 2


@pytest.mark.parametrize('failure', ['audit', 'empty', 'source_changed', 'fx_changed'])
def test_no_status_hook_does_not_bypass_data_checks(loader, failure):
    target, client = loader
    before = write(target)
    client.failure = failure
    kind, cfg, catalog, table = target
    with pytest.raises(ValueError):
        module(kind, 'runtime').load_day(cfg, catalog, client, day=DAY, manifest='no-logs')
    assert table.refresh().current_snapshot().snapshot_id == before['snapshot_id']


@pytest.mark.parametrize("failure", ["source_changed", "fx_changed", "interrupted", "payload", "audit", "metadata", "empty"])
def test_loader_failure_keeps_previous_day(loader, failure):
    target, client = loader
    # Настройки пакета не допускают два ключа в одной порции, прежний день содержит один.
    before = write(target)
    client.failure = failure
    with pytest.raises((ValueError, RuntimeError)):
        load(loader)
    table = target[3].refresh()
    assert table.current_snapshot().snapshot_id == before["snapshot_id"]
    assert table.scan().to_arrow().num_rows == 1
    if client.streamed:
        assert client.closed


def test_readiness_checked_before_any_source_query(loader):
    with pytest.raises(ValueError, match="Upstream"):
        load(loader, ready=lambda day: False)
    assert loader[1].queries == []


def test_readiness_lost_before_commit(loader):
    calls = []
    def ready(day):
        calls.append(day)
        return len(calls) == 1
    with pytest.raises(ValueError, match="Source/FX"):
        load(loader, ready=ready)
    assert calls == [DAY, DAY]
    assert loader[0][3].refresh().current_snapshot() is None


@pytest.mark.parametrize("bad", ["manifest", "limits", "table", "partition"])
def test_preflight_blocks_before_extraction(loader, bad):
    (kind, cfg, catalog, _), client = loader
    if bad == "limits":
        cfg["runtime"]["max_batch_rows"] = 0
    if bad == "table":
        cfg["table"]["name"] = "absent_table"
    if bad == "partition":
        table = catalog.load_table(module(kind, "preparation").target_ref(cfg, catalog.name))
        with table.update_spec() as spec:
            spec.remove_field("date")
    with pytest.raises(ValueError):
        load(loader, manifest="" if bad == "manifest" else "capture-1")
    assert client.queries == []


def test_no_fx_preserves_raw_and_null_usd(loader):
    target, client = loader
    client.fx.update(fx_rate_date=None, fx_rate_uzs_per_usd=None, fx_rate_source="unavailable")
    for name in client.source.column_names:
        if name.endswith("_usd"):
            client.source = client.source.set_column(client.source.column_names.index(name), name,
                                                    pa.nulls(client.source.num_rows, pa.float64()))
    load(loader)
    result = target[3].refresh().scan().to_arrow()
    assert all(result[name].null_count == 2 for name in result.column_names if name.endswith("_usd"))


@pytest.mark.parametrize("kind", ["finance"])
def test_native_decimal_metadata_rejects_float_without_rounding(kind):
    source = raw(kind)
    rows, columns = wire(source, kind)
    name = "sales_gmv" if kind == "sales" else "finance_gmv_net"
    index = [n for n, _ in columns].index(name)
    rows[0][index] = 0.1
    with pytest.raises(ValueError, match="значения source"):
        module(kind, "runtime").source_arrow(rows, columns)


@pytest.mark.parametrize("kind", ["finance"])
def test_native_decimal_high_precision_survives_small_context(kind):
    from decimal import localcontext
    rows, columns = wire(raw(kind), kind)
    name = "sales_gmv" if kind == "sales" else "finance_gmv_net"
    index = [n for n, _ in columns].index(name)
    exact = Decimal("99999999999999999999999999999999999999")
    rows[0][index] = exact
    with localcontext() as context:
        context.prec = 6
        result = module(kind, "runtime").source_arrow(rows, columns)
        assert result[name][0].as_py() == exact
