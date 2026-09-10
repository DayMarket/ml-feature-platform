"""Атомарный seller-sales день, точные controls между порциями и source/FX guards."""

from datetime import date, timedelta
from decimal import Decimal
from importlib import import_module
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, fx, raw as sales_raw
from ci_test.test_demand_sales_finance_runtime import wire

ROOT = Path(__file__).resolve().parents[1]
PATH = "layers/silver/sku_id_seller_key/demand_seller_sales_observed_daily/v1"


def mod(name):
    return import_module(PATH.replace("/", ".") + ".job." + name)


def config():
    return yaml.safe_load((ROOT / PATH / "config.yaml").read_text())


def schema():
    types = {"DATE": pa.date32(), "BIGINT": pa.int64(), "DOUBLE": pa.float64(),
             "STRING": pa.string(), "TIMESTAMP": pa.timestamp("us"), "DECIMAL(38,0)": pa.decimal128(38, 0)}
    ddl = (ROOT / PATH / "migrations/create_table.sql").read_text()
    fields = re.findall(r"^    (\w+) (DATE|BIGINT|DOUBLE|STRING|TIMESTAMP|DECIMAL\(38,0\))( NOT NULL)? COMMENT", ddl, re.M)
    assert len(fields) == 42
    return pa.schema([pa.field(n, types[t], nullable=not required) for n, t, required in fields])


def raw(**changes):
    base = sales_raw("sales").to_pylist()[0]
    base.update(seller_key="seller:7", seller_id=7, sku_sales_orders=1, sku_sales_order_items=1)
    base.update(changes)
    target = schema()
    names = mod("preparation").RAW_COLUMNS
    return pa.Table.from_arrays([
        pa.array([base[n]], type=pa.timestamp("us", "UTC") if n == "source_updated_at" else target.field(n).type)
        for n in names], names=names)


def prepared(source=None, target=None, day=DAY):
    return mod("preparation").prepare_batch(raw() if source is None else source,
        schema() if target is None else target, day=day,
        fx=fx() | {"date": day, "fx_rate_date": day},
        manifest="capture-1", version="v1", ingested_at=CAPTURE)


@pytest.fixture
def env(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog
    cfg = config()
    cat = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                     warehouse=(tmp_path / "warehouse").as_uri())
    cat.create_namespace("silver")
    table = cat.create_table(mod("preparation").target_ref(cfg, cat.name), schema=schema())
    with table.update_spec() as spec:
        spec.add_identity("date")
    yield cfg, cat, table
    cat.engine.dispose()


def write(env, sources=None, *, verify=lambda: True, expected=None, day=DAY, **kwargs):
    cfg, cat, table = env
    sources = [raw(date=day)] if sources is None else sources
    return mod("writer").write_day(cfg, cat,
        (prepared(source, table.schema().as_arrow(), day=day) for source in sources),
        day=day, expected_rows=len(sources) if expected is None else expected,
        manifest="capture-1", version="v1", ingested_at=CAPTURE, verify_source=verify, **kwargs)


def resume(env):
    cfg, cat, _ = env
    return mod("checkpoint").resume_day(cfg, cat, day=DAY, manifest="capture-1", version="v1")


def shared():
    return [raw(seller_id=sid, seller_key=key, sku_sales_orders=2, sku_sales_order_items=3)
            for sid, key in [(10, "seller:10"), (2, "seller:2"), (None, "unknown")]]


def test_parquet_schema_raw_usd_and_unknown(tmp_path):
    result = prepared(raw(seller_id=None, seller_key="unknown", sales_marketplace_promo_value=Decimal(-20),
                          sales_marketplace_promo_value_usd=-2.0))
    path = tmp_path / "seller.parquet"
    pq.write_table(result, path)
    assert pq.read_table(path).equals(result)
    assert result["seller_id"].to_pylist() == [None]
    assert result["sales_marketplace_promo_value"].to_pylist() == [Decimal(-20)]


@pytest.mark.parametrize("changes", [
    {"seller_id": 0, "seller_key": "unknown"}, {"seller_id": -1, "seller_key": "seller:-1"},
    {"seller_id": None, "seller_key": "seller:7"}, {"seller_id": 7, "seller_key": "unknown"},
    {"sku_sales_orders": None}, {"sku_sales_orders": 0}, {"sales_orders": None},
    {"sales_orders": 2}, {"sales_gmv_dbs": None, "sales_gmv_dbs_usd": None},
    {"sales_gmv_usd": 2.0},
])
def test_invalid_attribution_counts_or_money_rejected(changes):
    with pytest.raises(ValueError):
        prepared(raw(**changes))


def test_strict_nullable_and_float_raw_rejected():
    target = schema()
    target = target.set(target.get_field_index("seller_id"), pa.field("seller_id", pa.int64(), nullable=False))
    with pytest.raises(ValueError, match="Nullable"):
        prepared(target=target)
    source = raw().set_column(raw().schema.get_field_index("sales_gmv"), "sales_gmv", pa.array([1.0]))
    with pytest.raises(ValueError, match="округлять"):
        prepared(source)


def test_unknown_fx_requires_null_usd():
    receipt = fx() | {"fx_rate_date": None, "fx_rate_uzs_per_usd": None, "fx_rate_source": "unavailable"}
    source = raw(**{name + "_usd": None for name in mod("preparation").MONEY})
    result = mod("preparation").prepare_batch(source, schema(), day=DAY, fx=receipt,
        manifest="capture-1", version="v1", ingested_at=CAPTURE)
    assert result["sales_gmv"].to_pylist() == [Decimal(10)]


def test_cross_batch_controls_and_resume(env):
    written = write(env, shared())
    assert written["rows_written"] == 3 and written["status"] == "written"
    assert "dq_status" not in written
    assert resume(env)["snapshot_id"] == written["snapshot_id"]
    assert set(env[2].refresh().refs()) == {"main"}


def test_earlier_date_and_removed_sellers(env):
    earlier = date(2026, 9, 5)
    write(env, day=earlier)
    write(env, shared())
    write(env)
    rows = env[2].refresh().scan().to_arrow().to_pylist()
    assert {(r["date"], r["seller_key"]) for r in rows} == {(earlier, "seller:7"), (DAY, "seller:7")}


@pytest.mark.parametrize("failure", ["different_controls", "incomplete_union", "duplicate", "order", "partial", "source"])
def test_bad_day_keeps_old_snapshot(env, failure):
    before = write(env)
    sources, expected = shared(), 3
    if failure == "different_controls":
        sources[1] = raw(seller_id=2, seller_key="seller:2", sku_sales_orders=1, sku_sales_order_items=3)
    elif failure == "incomplete_union":
        sources, expected = sources[:1], 1
    elif failure == "duplicate":
        sources[1] = sources[0]
    elif failure == "order":
        sources[:2] = reversed(sources[:2])
    elif failure == "partial":
        sources = sources[:1]
    with pytest.raises(ValueError):
        write(env, sources, expected=expected, verify=lambda: failure != "source")
    assert env[2].refresh().current_snapshot().snapshot_id == before["snapshot_id"]


def test_missing_last_sku_control_is_checked(env):
    write(env)
    sources = [raw(), raw(sku_id=2, sku_sales_orders=2, sku_sales_order_items=2)]
    with pytest.raises(ValueError, match="границ"):
        write(env, sources)


def test_expected_target_blocks_concurrent_preflight_change(env):
    before = write(env)
    with pytest.raises(ValueError, match="Target"):
        write(env, expected_metadata_location="stale-metadata")
    assert env[2].refresh().current_snapshot().snapshot_id == before["snapshot_id"]


def test_concurrent_write_during_verify_is_not_overwritten(env):
    from pyiceberg.exceptions import CommitFailedException
    write(env)
    competing = {}
    def verify():
        competing["receipt"] = write(env, [raw(sku_id=99)])
        return True
    with pytest.raises(CommitFailedException):
        write(env, shared(), verify=verify)
    assert env[2].refresh().current_snapshot().snapshot_id == competing["receipt"]["snapshot_id"]
    assert env[2].scan().to_arrow()["sku_id"].to_pylist() == [99]


def test_source_generator_failure_preserves_snapshot(env):
    before = write(env)
    def sources():
        yield shared()[0]
        raise RuntimeError("source stopped")
    with pytest.raises(RuntimeError, match="stopped"):
        write(env, sources(), expected=3)
    assert env[2].refresh().current_snapshot().snapshot_id == before["snapshot_id"]


def test_same_count_and_key_changed_data_cannot_resume(env):
    from pyiceberg.expressions import EqualTo
    write(env)
    table = env[2].refresh()
    changed = prepared(raw(sales_payment_value=Decimal(90), sales_payment_value_usd=9.0),
                       table.schema().as_arrow())
    table.overwrite(changed, overwrite_filter=EqualTo("date", DAY))
    assert resume(env) is None


def test_lost_ack_resumes_existing_commit(env, monkeypatch):
    monkeypatch.setattr(mod("writer"), "verify_proof", lambda *args: (_ for _ in ()).throw(RuntimeError("lost ack")))
    with pytest.raises(RuntimeError, match="lost ack"):
        write(env, shared())
    assert resume(env)["rows_written"] == 3


class Client:
    def __init__(self, cfg, source):
        self.config, self.source = cfg, source
        self.failure = None
        self.coverage_calls = self.fx_calls = 0
        self.closed = self.streamed = False

    def execute(self, sql):
        query = mod("query")
        if sql == query.coverage_totals_query(self.config, DAY):
            self.coverage_calls += 1
            values = [self.source.num_rows]
            values += [sum(int(v) for v in self.source[n].to_pylist()) for n in query.TOTAL_COLUMNS]
            values += [CAPTURE]
            if self.failure == "source_changed" and self.coverage_calls > 1:
                values[1] += 1
            return [values]
        if sql == query.coverage_query(self.config, DAY):
            count = sum(self.source["sales_order_items"].to_pylist())
            return [[count, count, self.source.num_rows, int(self.failure == "invalid_seller"),
                     sum(self.source["sales_units"].to_pylist()),
                     Decimal(sum(int(v) for v in self.source["sales_gmv"].to_pylist()))]]
        if sql == query.fx_query(DAY):
            self.fx_calls += 1
            receipt = fx()
            if self.failure == "fx_changed" and self.fx_calls > 1:
                receipt["fx_rate_uzs_per_usd"] *= 2
            return [list(receipt.values())]
        raise AssertionError(sql)

    def execute_iter(self, sql, **kwargs):
        chunk_size = self.config["runtime"]["max_batch_rows"]
        assert kwargs["with_column_types"] and kwargs["chunk_size"] == chunk_size
        assert sql == mod("query").source_query(self.config, DAY, fx_available=True)
        self.streamed = True
        rows, columns = wire(self.source, "sales")
        try:
            buffer = [columns]
            for i, row in enumerate(rows):
                if self.failure == "interrupted" and i == 1:
                    raise RuntimeError("source interrupted")
                if self.failure == "payload" and i == 0:
                    names = [name for name, _ in columns]
                    row[names.index("sales_payment_value")] += 10
                    row[names.index("sales_payment_value_usd")] += 1
                buffer.append(row)
                if len(buffer) == chunk_size:
                    yield buffer
                    buffer = []
            if buffer:
                yield buffer
        finally:
            self.closed = True


def loader(env, monkeypatch, failure=None):
    cfg, cat, _ = env
    # chunk_size=1 у clickhouse-driver отключает группировку и возвращает
    # метаданные/строки поэлементно; production-контракт использует порции > 1.
    cfg["runtime"]["max_batch_rows"] = 2
    monkeypatch.setattr(mod("runtime"), "utc_now", lambda: CAPTURE + timedelta(seconds=1))
    client = Client(cfg, pa.concat_tables(shared()))
    client.failure = failure
    return client, lambda: mod("runtime").load_day(cfg, cat, client, day=DAY, manifest="capture-1")


def test_native_stream_checks_controls_and_closes(env, monkeypatch):
    client, load = loader(env, monkeypatch)
    result = load()
    assert result["rows_written"] == 3 and client.closed
    assert client.coverage_calls == client.fx_calls == 2
    assert env[2].refresh().scan().to_arrow()["seller_id"].null_count == 1


@pytest.mark.parametrize("failure", ["invalid_seller", "source_changed", "fx_changed", "payload", "interrupted"])
def test_source_or_fx_error_keeps_previous_day(env, monkeypatch, failure):
    old = write(env)
    client, load = loader(env, monkeypatch, failure)
    with pytest.raises((ValueError, RuntimeError)):
        load()
    assert env[2].refresh().current_snapshot().snapshot_id == old["snapshot_id"]
    if client.streamed:
        assert client.closed
    if failure == "invalid_seller":
        assert not client.streamed
