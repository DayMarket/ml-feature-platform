"""Demand-витрины: DAG-контракт, SQL и запись в Iceberg без внешних подключений."""

from __future__ import annotations

import ast
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from importlib import import_module
from pathlib import Path
import re
from types import SimpleNamespace

import pyarrow as pa
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DAILY = {
    "stock": "layers/silver/sku_id/demand_stock_daily/v1",
    "seller_sales": "layers/silver/sku_id_seller_key/demand_seller_sales_observed_daily/v1",
    "finance": "layers/silver/sku_id_seller_key/demand_finance_daily/v1",
    "sales": "layers/silver/sku_id/demand_sales_daily/v1",
    "observed": "layers/gold/sku_id/demand_observed_daily/v1",
}
SNAPSHOT = {
    "calendar": "layers/silver/date/demand_calendar/v1",
    "events": "layers/silver/event_code/demand_event_calendar/v1",
    "calendar_daily": "layers/gold/date/demand_calendar_daily/v1",
    "catalog_seller": "layers/silver/seller_id/demand_catalog_seller/v1",
    "catalog_sku": "layers/silver/sku_id/demand_catalog_sku/v1",
    "catalog_tree": "layers/silver/level_node_id/demand_catalog_tree/v1",
}
PATHS = {**DAILY, **SNAPSHOT}
SENSORS = {
    "sales": {"seller_sales": 0},
    "observed": {"sales": 60, "stock": 60},
    "events": {"calendar": 10},
    "calendar_daily": {"calendar": 60, "events": 50},
    "catalog_sku": {"catalog_seller": 0},
    "catalog_tree": {"catalog_sku": 0},
}
TYPES = {"DATE": pa.date32(), "BIGINT": pa.int64(), "INT": pa.int32(), "DOUBLE": pa.float64(),
         "STRING": pa.string(), "TIMESTAMP": pa.timestamp("us"), "BOOLEAN": pa.bool_(),
         "DECIMAL(38,0)": pa.decimal128(38, 0)}
CAPTURE = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)


def config(kind):
    return yaml.safe_load((ROOT / PATHS[kind] / "config.yaml").read_text(encoding="utf-8"))


def job(kind, name):
    return import_module(PATHS[kind].replace("/", ".") + ".job." + name)


def schema(kind):
    ddl = (ROOT / PATHS[kind] / "migrations/create_table.sql").read_text(encoding="utf-8")
    fields = re.findall(r"^\s+(\w+) (DECIMAL\(38,0\)|[A-Z]+)( NOT NULL)? COMMENT", ddl, re.M)
    return pa.schema([pa.field(name, TYPES[kind_], nullable=not required) for name, kind_, required in fields])


def fake_value(field, day):
    dtype = field.type
    if pa.types.is_date(dtype):
        return day
    if pa.types.is_boolean(dtype):
        return True
    if pa.types.is_integer(dtype):
        return 3
    if pa.types.is_decimal(dtype):
        return Decimal(7)
    if pa.types.is_floating(dtype):
        return 0.5
    if pa.types.is_timestamp(dtype):
        return CAPTURE
    return "x"


# ---------------------------------------------------------------- DAG-контракт


@pytest.mark.parametrize("kind", list(PATHS))
def test_dag_has_terminal_dq_and_stats_with_config_templates(kind):
    text = (ROOT / PATHS[kind] / "dag.py").read_text(encoding="utf-8")
    cfg = config(kind)
    assert ">> [dq_task, stats_task]" in text
    assert "dq_task >>" not in text and "stats_task >>" not in text
    assert cfg["dq"]["partition_date_template"] == cfg["feature_stats"]["partition_date_template"]
    assert 'task_ids="write"' in cfg["dq"]["partition_date_template"]
    for legacy in ("reference", "receipt", "run_guard", "range_"):
        assert legacy not in text, f"{kind}: осталось {legacy}"


@pytest.mark.parametrize("kind", list(DAILY))
def test_daily_entities_use_week_window_and_date_list(kind):
    cfg = config(kind)
    assert cfg["runtime"]["refresh_days"] == 7
    assert cfg["dq"]["partition_date_template"] == '{{ ti.xcom_pull(task_ids="write") | join(",") }}'
    text = (ROOT / PATHS[kind] / "dag.py").read_text(encoding="utf-8")
    assert '"start": Param(' in text and '"end": Param(' in text


@pytest.mark.parametrize("kind", list(SNAPSHOT))
def test_snapshot_entities_pass_capture_timestamp(kind):
    cfg = config(kind)
    assert cfg["dq"]["partition_granularity"] == "timestamp"
    assert cfg["dq"]["partition_date_template"] == '{{ ti.xcom_pull(task_ids="write")["ingested_at"] }}'


@pytest.mark.parametrize("kind,upstreams", SENSORS.items())
def test_sensors_wait_for_upstream_dq_only_in_scheduled_runs(kind, upstreams):
    text = (ROOT / PATHS[kind] / "dag.py").read_text(encoding="utf-8")
    tree = ast.parse(text)
    sensors = [node for node in ast.walk(tree)
               if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ExternalTaskSensor"]
    assert len(sensors) == len(upstreams)
    for sensor in sensors:
        keywords = {item.arg: item.value for item in sensor.keywords}
        assert ast.literal_eval(keywords["external_task_id"]) == "dq"
        assert ast.literal_eval(keywords["task_id"]) in text.split("def upstream_gate")[1]
    assert '== "scheduled"' in text and 'trigger_rule="none_failed"' in text
    own = config(kind)["dag"]["schedule"].split()
    for upstream, minutes in upstreams.items():
        theirs = config(upstream)["dag"]["schedule"].split()
        delta = (int(own[1]) * 60 + int(own[0])) - (int(theirs[1]) * 60 + int(theirs[0]))
        assert delta == minutes, f"{kind}: расписание {upstream} разошлось с execution_delta"


def render_window(kind, params, run_after):
    jinja2 = pytest.importorskip("jinja2")
    module = ast.parse((ROOT / PATHS[kind] / "dag.py").read_text(encoding="utf-8"))
    env = {"RUNTIME": config(kind)["runtime"]}
    for node in module.body:
        if isinstance(node, ast.Assign) and node.targets[0].id in ("RUN_DATE", "START", "END"):
            env[node.targets[0].id] = eval(compile(ast.Expression(node.value), "dag", "eval"), {}, env)

    def ds_add(value, days):
        return (date.fromisoformat(value) + timedelta(days=days)).isoformat()

    context = {"params": params, "macros": SimpleNamespace(ds_add=ds_add),
               "dag_run": SimpleNamespace(run_after=run_after)}
    return tuple(jinja2.Template(env[name]).render(**context) for name in ("START", "END"))


@pytest.mark.parametrize("kind", list(DAILY))
def test_default_window_is_last_seven_completed_days(kind):
    run_after = datetime(2026, 9, 15, 4, tzinfo=timezone.utc)
    assert render_window(kind, {"start": None, "end": None}, run_after) == ("2026-09-08", "2026-09-14")
    manual = {"start": "2026-08-01", "end": "2026-08-31"}
    assert render_window(kind, manual, run_after) == ("2026-08-01", "2026-08-31")


def test_day_range_is_inclusive_and_rejects_unfinished_days():
    runtime = job("stock", "runtime")
    today = date(2026, 9, 15)
    assert runtime.day_range("2026-09-13", "2026-09-14", today=today) == [date(2026, 9, 13), date(2026, 9, 14)]
    for start, end in (("2026-09-14", "2026-09-13"), ("2026-09-14", "2026-09-15"), ("bad", "2026-09-01")):
        with pytest.raises(ValueError):
            runtime.day_range(start, end, today=today)


def test_generated_map_is_current():
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "scripts/generate_feature_platform_map.py", "--check"],
                            cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


# ------------------------------------------------------------------------ SQL


def test_seller_sales_query_keeps_exact_sku_distincts_and_fx():
    sql = job("seller_sales", "query").source_query(config("seller_sales"), date(2026, 9, 1))
    assert "GROUPING SETS ((sku_id, seller_key), (sku_id))" in sql
    assert "maxIf(sales_orders, is_sku_total = 1) OVER (PARTITION BY sku_id)" in sql
    assert "toDateTime('2026-08-31 19:00:00', 'UTC')" in sql
    assert "order_item_status NOT IN ('CREATED', 'NOT_CREATED')" in sql
    assert sql.count("/ fx_rate AS") == 10 and "latest_available" in sql


def test_finance_query_uses_event_date_without_final():
    sql = job("finance", "query").source_query(config("finance"), date(2026, 9, 6))
    assert "WHERE dt = fact_date" in sql and "toDate('2026-09-06') AS fact_date" in sql
    assert "FINAL" not in sql and "GROUP BY sku_id, seller_key, seller_id" in sql
    assert sql.count("/ fx_rate AS") == 20


def test_sales_rollup_does_not_sum_distinct_counts():
    sql = job("sales", "query").source_query('"t"', date(2026, 9, 1))
    assert "max(sku_sales_orders) AS sales_orders" in sql
    assert "sum(sales_orders)" not in sql
    assert "IF(count(sales_gmv) = count(*), sum(sales_gmv)) AS sales_gmv" in sql
    assert 'GROUP BY "date", sku_id' in sql


def test_observed_query_is_full_outer_join_of_one_day():
    sql = job("observed", "query").source_query("S", "K", date(2026, 9, 1))
    assert "FULL OUTER JOIN" in sql and sql.count("DATE '2026-09-01'") == 3
    assert "s.sku_id IS NOT NULL AS sales_component_present" in sql
    assert "s.ingested_at AS sales_ingested_at" in sql
    assert "SELECT sku_id, purchase_price_eod, sell_price_eod, full_price_eod FROM K" in sql
    assert "k.purchase_price_eod,\n    k.sell_price_eod,\n    k.full_price_eod\n" in sql


def test_stock_query_keeps_positive_eod_and_nulls_zero_prices():
    sql = job("stock", "query").source_query(config("stock"), date(2026, 9, 1))
    assert "(quantity_active_eod > 0 OR quantity_fbs_eod > 0)" in sql
    for name in ("purchase_price_eod", "sell_price_eod", "full_price_eod"):
        assert f"toInt64(nullIf({name}, 0)) AS {name}" in sql


# ------------------------------------------------------------ MDM и категории


def test_golden_chains_cycles_and_conflicts():
    mapping = job("catalog_sku", "mapping")
    golden = pa.table({
        "golden_sku_id": ["a", "b", "c", "x", "y", "t1", "t2"],
        "is_merged": pa.array([1, 1, 0, 1, 1, 0, 0], pa.int8()),
        "merged_into": ["b", "c", "", "y", "x", "", ""],
    })
    links = pa.table({
        "sku_id": pa.array([1, 2, 3, 3, 4, 4, 5], pa.int64()),
        "golden_sku_id": ["a", "x", "t1", "t2", "a", "c", "missing"],
    })
    result = mapping.resolve_sku_links(links, golden).sort_by("sku_id").to_pylist()
    assert result == [
        {"sku_id": 1, "golden_sku_id": "c", "golden_mapping_status": "matched"},
        {"sku_id": 2, "golden_sku_id": None, "golden_mapping_status": "conflict"},
        {"sku_id": 3, "golden_sku_id": None, "golden_mapping_status": "conflict"},
        {"sku_id": 4, "golden_sku_id": "c", "golden_mapping_status": "matched"},
        {"sku_id": 5, "golden_sku_id": None, "golden_mapping_status": "unavailable"},
    ]
    broken = golden.set_column(2, "merged_into", pa.array(["nowhere", "c", "", "y", "x", "", ""]))
    with pytest.raises(ValueError, match="merge-целей"):
        mapping.resolve_sku_links(links, broken)


def test_category_node_with_two_parents_marks_all_its_paths():
    mapping = job("catalog_sku", "mapping")
    categories = pa.table({
        "category_id": [10, 11, 12],
        "category_path_status": ["valid", "valid", "valid"],
        "market": ["market"] * 3,
        "l1": ["l1:1", "l1:2", "l1:3"],
        "l2": ["l2:5", "l2:5", "l2:6"],
        "l3": ["l3:5", "l3:5", "l3:6"],
        "l4": ["l4:5", "l4:5", "l4:6"],
        "l5": ["l5:5", "l5:5", "l5:6"],
        "leaf": ["leaf:10", "leaf:11", "leaf:12"],
    })
    result = mapping.mark_category_conflicts(categories)
    assert result["category_path_status"].to_pylist() == ["conflict", "conflict", "valid"]
    assert result["l1"].to_pylist() == [None, None, "l1:3"]


# ------------------------------------------------------- запись в Iceberg (SQLite)


@pytest.fixture
def catalog(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog

    result = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                        warehouse=(tmp_path / "warehouse").as_uri())
    for namespace in ("silver", "gold"):
        result.create_namespace(namespace)
    for kind in PATHS:
        table = config(kind)["table"]
        result.create_table((table["schema"], table["name"]), schema(kind))
    return result


def rows_of(catalog, kind):
    table = config(kind)["table"]
    return catalog.load_table((table["schema"], table["name"])).scan().to_arrow()


class ClickHouse:
    """execute_iter/execute как у clickhouse_driver: заголовок колонок, затем строки."""

    def __init__(self, respond):
        self.respond = respond
        self.queries = []

    def execute_iter(self, sql, with_column_types, chunk_size, settings):
        self.queries.append(sql)
        names, rows = self.respond(sql)
        header = [(name, "?") for name in names]
        return iter([[header, *rows[:1]], rows[1:]])

    def execute(self, sql, with_column_types=False):
        self.queries.append(sql)
        names, rows = self.respond(sql)
        return (rows, [(name, "?") for name in names]) if with_column_types else rows


class Trino:
    def __init__(self, respond):
        self.respond = respond
        self.queries = []

    def cursor(self):
        return self

    def execute(self, sql):
        self.queries.append(sql)
        self.names, self.rows = self.respond(sql)
        self.description = [(name,) for name in self.names]

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def fetchmany(self, size):
        rows, self.rows = self.rows[:size], self.rows[size:]
        return rows

    def close(self):
        pass


def source_rows(kind, day, skip=(), overrides=None):
    fields = [field for field in schema(kind) if field.name not in skip]
    row = {field.name: fake_value(field, day) for field in fields}
    row.update(overrides or {})
    return [field.name for field in fields], row


TECH = ("source_manifest_id", "source_contract_version", "ingested_at")


@pytest.mark.parametrize("kind", ["stock", "seller_sales", "finance"])
def test_clickhouse_daily_entities_overwrite_each_day(kind, catalog):
    def respond(sql):
        day = date.fromisoformat(sql.split("toDate('")[1][:10])
        names, row = source_rows(kind, day, TECH)
        rows = []
        for sku in (1, 2):
            row = {**row, "sku_id": sku}
            rows.append(tuple(row[name] for name in names))
        return names, rows

    runtime = job(kind, "runtime")
    client = ClickHouse(respond)
    assert runtime.load_range(config(kind), "2026-09-01", "2026-09-02", run_id="r1",
                              client=client, catalog=catalog) == ["2026-09-01", "2026-09-02"]
    assert runtime.load_range(config(kind), "2026-09-02", "2026-09-02", run_id="r2",
                              client=client, catalog=catalog) == ["2026-09-02"]
    data = rows_of(catalog, kind)
    assert data.num_rows == 4
    manifests = {(row["date"], row["source_manifest_id"]) for row in data.to_pylist()}
    assert manifests == {(date(2026, 9, 1), "r1"), (date(2026, 9, 2), "r2")}


def test_empty_source_day_does_not_erase_partition(catalog):
    runtime = job("stock", "runtime")
    names = ["date", "sku_id", "purchase_price_eod", "sell_price_eod", "full_price_eod"]
    full = ClickHouse(lambda sql: (names, [(date(2026, 9, 1), 1, 900, 1000, None)]))
    runtime.load_range(config("stock"), "2026-09-01", "2026-09-01", run_id="r1", client=full, catalog=catalog)
    empty = ClickHouse(lambda sql: (names, []))
    with pytest.raises(ValueError, match="0 строк"):
        runtime.load_range(config("stock"), "2026-09-01", "2026-09-01", run_id="r2", client=empty, catalog=catalog)
    data = rows_of(catalog, "stock")
    assert data.num_rows == 1
    assert data.select(names[2:]).to_pylist() == [{"purchase_price_eod": 900, "sell_price_eod": 1000,
                                                    "full_price_eod": None}]


def test_sales_rollup_and_observed_join_write_through_trino(catalog):
    def seed(kind, day):
        names, row = source_rows(kind, day)
        target = config(kind)["table"]
        table = catalog.load_table((target["schema"], target["name"]))
        table.append(pa.Table.from_pylist([row], schema=table.schema().as_arrow()))

    seed("sales", date(2026, 8, 1))
    seed("stock", date(2026, 8, 1))

    def sales_respond(sql):
        day = date.fromisoformat(sql.split("DATE '")[1][:10])
        names, row = source_rows("sales", day, TECH)
        return names, [tuple(row[name] for name in names)]

    sales = job("sales", "runtime")
    connection = Trino(sales_respond)
    sources = {"sales": config("sales"), "stock": config("stock")}
    sales.load_range(config("sales"), config("seller_sales"), str(ROOT), "2026-09-01", "2026-09-01",
                     run_id="s1", connection=connection, catalog=catalog)
    assert '"dwh-iceberg"."silver"."feature_platform_demand_seller_sales_observed_daily"' in connection.queries[0]
    assert rows_of(catalog, "sales").num_rows == 2

    def observed_respond(sql):
        if sql.startswith("SELECT (SELECT count(*)"):
            return ["sales", "stock"], [(5, 7)]
        skip = {*TECH, "sales_snapshot_id", "sales_table_uuid", "stock_snapshot_id", "stock_table_uuid"}
        names, row = source_rows("observed", date(2026, 9, 1), skip)
        return names, [tuple(row[name] for name in names)]

    observed = job("observed", "runtime")
    connection = Trino(observed_respond)
    observed.load_range(config("observed"), sources, str(ROOT), "2026-09-01", "2026-09-01",
                        run_id="o1", connection=connection, catalog=catalog)
    assert "FOR VERSION AS OF" in connection.queries[1]
    row = rows_of(catalog, "observed").to_pylist()[0]
    assert row["sales_snapshot_id"] > 0 and row["source_manifest_id"] == "o1"

    empty_stock = Trino(lambda sql: (["sales", "stock"], [(5, 0)]))
    with pytest.raises(ValueError, match="нет входных строк"):
        observed.load_range(config("observed"), sources, str(ROOT), "2026-09-01", "2026-09-01",
                            run_id="o2", connection=empty_stock, catalog=catalog)


def test_calendar_events_and_gold_calendar_replace_tables(catalog):
    day = date(2026, 1, 1)
    calendar_names, calendar_row = source_rows("calendar", day, ("calendar_id", "source_manifest_id", "ingested_at"))
    calendar_rows = [
        tuple({**calendar_row, "date": day + timedelta(days=offset), "is_public_holiday": offset == 0}[name]
              for name in calendar_names)
        for offset in range(5)
    ]
    runtime = job("calendar", "runtime")
    receipt = runtime.load(config("calendar"), run_id="c1",
                           client=ClickHouse(lambda sql: (calendar_names, calendar_rows)), catalog=catalog)
    assert receipt["rows"] == 5 and len(receipt["ingested_at"]) == 19

    promo_names, promo = source_rows("events", day, ("calendar_id", "source_manifest_id", "ingested_at"))
    promo.update(event_code="marketing_sale:5", source_kind="marketing_sale", source_event_id="5")

    def events_respond(sql):
        if "uniqExact(id)" in sql:
            return ["promos", "duplicate_ids", "invalid_ids", "invalid_intervals"], [(1, 0, 0, 0)]
        return promo_names, [tuple({**promo, "date": day + timedelta(days=1 + offset * 30)}[name]
                                   for name in promo_names) for offset in range(2)]

    events = job("events", "runtime")
    events.load(config("events"), config("calendar"), run_id="e1",
                client=ClickHouse(events_respond), catalog=catalog)
    stored = rows_of(catalog, "events").sort_by("date").to_pylist()
    assert [(row["date"], row["event_code"], row["calendar_id"]) for row in stored] == [
        (date(2026, 1, 1), "calendar:uz_official:2026-01-01", "uz_official"),
        (date(2026, 1, 2), "marketing_sale:5", None),
    ]

    bad = ClickHouse(lambda sql: (["promos", "duplicate_ids", "invalid_ids", "invalid_intervals"], [(2, 1, 0, 0)]))
    with pytest.raises(ValueError, match="duplicate_ids=1"):
        events.load(config("events"), config("calendar"), run_id="e2", client=bad, catalog=catalog)

    gold_skip = ("calendar_snapshot_id", "events_snapshot_id", "source_manifest_id", "ingested_at")
    gold_names, gold_row = source_rows("calendar_daily", day, gold_skip)
    gold = job("calendar_daily", "runtime")
    connection = Trino(lambda sql: (gold_names, [tuple(gold_row[name] for name in gold_names)]))
    gold.load(config("calendar_daily"), {"calendar": config("calendar"), "events": config("events")},
              str(ROOT), run_id="g1", connection=connection, catalog=catalog)
    assert "LEFT JOIN" in connection.queries[0] and "BIG_SALE" in connection.queries[0]
    assert rows_of(catalog, "calendar_daily").to_pylist()[0]["calendar_snapshot_id"] > 0


def test_catalogs_seller_sku_tree(catalog):
    seller_names = ["seller_id", "source_master_seller_id", "master_seller_id", "seller_mapping_status",
                    "has_master", "is_1p", "seller_registered_at"]
    seller_rows = [(7, "m1", "m1", "matched", True, False, CAPTURE),
                   (8, "", "8", "unmatched", False, None, None)]
    seller = job("catalog_seller", "runtime")
    seller.load(config("catalog_seller"), run_id="cs1",
                client=ClickHouse(lambda sql: (seller_names, seller_rows)), catalog=catalog)
    collision = seller_rows + [(9, "8", "8", "matched", True, None, None)]
    with pytest.raises(ValueError, match="fallback"):
        seller.load(config("catalog_seller"), run_id="cs2",
                    client=ClickHouse(lambda sql: (seller_names, collision)), catalog=catalog)

    def sku_respond(sql):
        if "FROM dict.sku" in sql:
            return (["sku_id", "product_id", "category_id", "seller_id", "shop_id", "sku_created_at", "sku_status"],
                    [(1, 10, 100, 7, 1, CAPTURE, "ACTIVE"), (2, 11, 999, 404, None, None, None)])
        if "FROM dict.category" in sql:
            names = ["category_id", *(f"raw_l{n}_category_id" for n in range(1, 7)), "l1_title", "leaf_title",
                     "category_path_status", "market", "l1", "l2", "l3", "l4", "l5", "leaf"]
            return names, [(100, 1, 2, 0, 0, 0, 0, "L1", "Leaf", "valid", "market",
                            "l1:1", "l2:2", "l3:2", "l4:2", "l5:2", "leaf:100")]
        if "argMax" in sql:
            return ["golden_sku_id", "is_merged", "merged_into"], [("g1", 0, "")]
        return ["sku_id", "golden_sku_id"], [(1, "g1")]

    sku = job("catalog_sku", "runtime")
    sku.load(config("catalog_sku"), config("catalog_seller"), run_id="k1",
             client=ClickHouse(sku_respond), catalog=catalog)
    rows = {row["sku_id"]: row for row in rows_of(catalog, "catalog_sku").to_pylist()}
    assert (rows[1]["unit_id"], rows[1]["master_seller_id"], rows[1]["leaf"]) == ("g:g1", "m1", "leaf:100")
    assert (rows[2]["unit_id"], rows[2]["category_path_status"], rows[2]["seller_mapping_status"]) == (
        "s:2", "missing", "unavailable")
    assert rows[1]["catalog_version"] == "catalog:cs1"

    tree_names = ["date", "level", "node_id", "level_code", "parent_id", "is_passthrough", "catalog_version"]
    tree_rows = [(date(2026, 9, 1), "market", "market", 0, None, False, "catalog:cs1"),
                 (date(2026, 9, 1), "l1", "l1:1", 1, "market", False, "catalog:cs1")]
    tree = job("catalog_tree", "runtime")
    connection = Trino(lambda sql: (tree_names, tree_rows))
    tree.load(config("catalog_tree"), config("catalog_sku"), str(ROOT), run_id="t1",
              connection=connection, catalog=catalog)
    assert "CROSS JOIN UNNEST" in connection.queries[0]
    assert rows_of(catalog, "catalog_tree").num_rows == 2
