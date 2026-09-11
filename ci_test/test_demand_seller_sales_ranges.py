"""Seller ranges: дневная атомарность, resume и строгий service preflight."""

from datetime import timedelta
from decimal import Decimal
import re

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_seller_sales_writer import CAPTURE, DAY, ROOT, fx, mod, raw, wire
from ci_test.test_demand_seller_sales_writer import env as env

DAYS = [DAY - timedelta(days=1), DAY]


def plan(cfg):
    return mod("ranges").build_request(cfg, run_id="manual", mode="manual",
        interval_start=f"{DAYS[0].isoformat()}T04:00:00Z",
        interval_end=f"{(DAY + timedelta(days=1)).isoformat()}T04:00:00Z",
        history_start=DAYS[0])


class DailyClient:
    def __init__(self, cfg):
        self.cfg, self.streams, self.calls = cfg, [], []
        self.missing = self.failed_day = None
        self.rate, self.delta = 10.0, {}
        self.mapping = {}
        query = mod("query")
        for day in DAYS:
            for name in ("coverage_totals_query", "coverage_query", "source_query", "fx_query"):
                sql = query.fx_query(day) if name == "fx_query" else (
                    query.source_query(cfg, day, fx_available=True) if name == "source_query"
                    else getattr(query, name)(cfg, day))
                self.mapping[sql] = (name, day)

    def source(self, day):
        row = raw(date=day).to_pylist()[0]
        row["sales_payment_value"] += Decimal(self.delta.get(day, 0))
        for name in mod("preparation").MONEY:
            row[name + "_usd"] = float(row[name]) / self.rate
        return raw(**row)

    def execute(self, sql, **kwargs):
        if kwargs.get("with_column_types"):
            assert sql == mod("orchestration").source_schema_query(self.cfg, DAYS[0])
            return [], wire(self.source(DAYS[0]), "sales")[1]
        name, day = self.mapping[sql]
        self.calls.append((name, day))
        source = self.source(day)
        if name == "coverage_totals_query":
            values = [0 if day == self.missing else source.num_rows]
            values += [sum(int(v) for v in source[n].to_pylist()) for n in mod("query").TOTAL_COLUMNS]
            return [values + [CAPTURE]]
        if name == "coverage_query":
            return [[1, 1, 1, 0, 1, Decimal(10)]]
        if name == "fx_query":
            return [list((fx() | {"date": day, "fx_rate_date": day, "fx_rate_uzs_per_usd": self.rate}).values())]
        raise AssertionError(name)

    def execute_iter(self, sql, **kwargs):
        name, day = self.mapping[sql]
        assert name == "source_query"
        self.streams.append(day)
        if day == self.failed_day:
            raise RuntimeError("interrupted day")
        rows, columns = wire(self.source(day), "sales")
        size = kwargs["chunk_size"]
        payload = [columns, *rows]
        for start in range(0, len(payload), size):
            yield payload[start:start + size]


def load(env, client):
    cfg, catalog, _ = env
    return mod("ranges").load_range(cfg, catalog, client, plan(cfg), preflight=lambda *args: True)


def test_range_resume_does_not_reextract_checked_days(env):
    client = DailyClient(env[0])
    first = load(env, client)
    second = load(env, client)
    assert first["snapshot_id"] == second["snapshot_id"]
    assert all(r["resumed"] for r in second["day_receipts"])
    assert client.streams == DAYS
    assert first["status"] == "written" and "dq_status" not in first


def test_missing_later_day_preserves_first_write(env):
    client = DailyClient(env[0])
    client.missing = DAYS[-1]
    with pytest.raises(ValueError, match="Пустой"):
        load(env, client)
    assert client.streams == [DAYS[0]]
    assert env[2].refresh().scan().count() == 1


def test_partial_manual_range_resumes_from_failed_day(env):
    client = DailyClient(env[0])
    client.failed_day = DAYS[-1]
    with pytest.raises(RuntimeError, match="interrupted"):
        load(env, client)
    assert env[2].refresh().scan().count() == 1
    client.failed_day = None
    result = load(env, client)
    assert [r["resumed"] for r in result["day_receipts"]] == [True, False]
    assert client.streams == [DAYS[0], DAYS[1], DAYS[1]]


@pytest.mark.parametrize("kind", ["money", "fx"])
def test_same_run_resume_does_not_rewrite_completed_days(env, kind):
    client = DailyClient(env[0])
    first = load(env, client)
    if kind == "money":
        client.delta[DAYS[0]] = 10
    else:
        client.rate = 20.0
    second = load(env, client)
    assert second["snapshot_id"] == first["snapshot_id"]
    assert all(receipt["resumed"] for receipt in second["day_receipts"])


def test_regular_includes_exactly_last_31_days(env):
    first = DAY - timedelta(days=40)
    request = mod("ranges").build_request(env[0], run_id="regular", mode="regular",
        interval_start=f"{(DAY - timedelta(days=1)).isoformat()}T04:00:00Z",
        interval_end=f"{DAY.isoformat()}T04:00:00Z", history_start=first)
    assert len(request["dates"]) == 31
    assert request["dates"][0] == (DAY - timedelta(days=31)).isoformat()


def services(catalog, *, invalid=False):
    types = {"DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "STRING": pa.string(),
             "INT": pa.int32(), "BIGINT": pa.int64(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_()}
    for relative in ("dq/results", "feature_stats/results"):
        cfg = yaml.safe_load((ROOT / relative / "config.yaml").read_text())["table"]
        ddl = (ROOT / relative / "migrations/create_table.sql").read_text()
        fields = re.findall(r"^    (\w+) (\w+)( NOT NULL)? COMMENT", ddl, re.M)
        schema = pa.schema([pa.field(name, types[kind], nullable=not required) for name, kind, required in fields])
        if invalid and relative == "dq/results":
            schema = pa.schema([pa.field("date", pa.date32())])
        catalog.create_table((cfg["schema"], cfg["name"]), schema=schema)


def test_connection_bridge_checks_service_ddl_and_source_metadata(env):
    cfg, catalog, table = env
    services(catalog)
    client, sql = DailyClient(cfg), []
    def query(value):
        sql.append(value)
        return []
    result = mod("orchestration").execute_range(cfg, ROOT, plan(cfg),
        catalog=catalog, client=client, queries={"trino_search": query})
    assert result["status"] == "written"
    assert len(sql) == 3 and all('"dwh-iceberg"."silver".' in q for q in sql)
    assert table.refresh().scan().count() == 2


def test_wrong_service_schema_fails_before_any_source_query(env):
    cfg, catalog, table = env
    services(catalog, invalid=True)
    client = DailyClient(cfg)
    with pytest.raises(ValueError, match="Служебная схема"):
        mod("orchestration").execute_range(cfg, ROOT, plan(cfg), catalog=catalog, client=client,
            queries={"trino_search": lambda sql: []})
    assert not client.calls and not client.streams
    assert table.refresh().current_snapshot() is None
