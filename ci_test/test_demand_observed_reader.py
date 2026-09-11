"""Bounded Trino DB-API reader: явные типы, exact snapshots, EOF и закрытие cursor."""

from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from importlib import import_module

import pyarrow as pa
import pytest

from ci_test.test_demand_observed_inputs import INPUTS, bind, payload
from ci_test.test_demand_observed_preparation import DAY, NOW, ROOT, PREP, source

READER = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.reader")


def description(schema, columns):
    types = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint", pa.float64(): "double",
             pa.decimal128(38, 0): "decimal(38,0)", pa.timestamp("us"): "timestamp(6)",
             pa.string(): "varchar", pa.large_string(): "varchar", pa.bool_(): "boolean"}
    return [(name, types[schema.field(name).type], None, None, None, None, None) for name in columns]


class Cursor:
    def __init__(self, rows, metadata, *, fail_at=None):
        self.rows, self.description = deepcopy(rows), metadata
        self.closed, self.sql, self.sizes = False, None, []
        self.offset, self.fail_at = 0, fail_at
    def execute(self, sql):
        self.sql = sql
    def fetchmany(self, size):
        self.sizes.append(size)
        if self.fail_at is not None and self.offset >= self.fail_at:
            raise RuntimeError("Trino stream interrupted")
        result = self.rows[self.offset:self.offset + size]
        self.offset += len(result)
        return result
    def close(self):
        self.closed = True


class Connection:
    def __init__(self, cursor):
        self.value, self.calls = cursor, 0
    def cursor(self):
        self.calls += 1
        return self.value


def fixture(kind="sales", ids=(1, 3, 5)):
    data = payload()
    source_config = data[1][kind]
    batch = source(kind, ids, source_manifest_id="capture",
                   source_contract_version=source_config["source"]["contract_version"],
                   sales_gmv=Decimal("12345678901234567890123456789012345678"),
                   sales_gmv_usd=None, price_sell_eod=0)
    inputs = INPUTS.day_inputs(bind(data), DAY)
    inputs[kind]["day_receipt"]["rows_written"] = len(ids)
    columns = PREP.SOURCE_FIELDS[kind]
    cursor = Cursor([[r[name] for name in columns] for r in batch.to_pylist()], description(batch.schema, columns))
    return data, batch, inputs, cursor


def read(fix, kind="sales", **options):
    data, batch, inputs, cursor = fix
    return READER.read_batches(Connection(cursor), kind, data[1][kind], ROOT, inputs[kind], batch.schema,
        day=DAY, max_batch_rows=options.get("max_batch_rows", 2), max_batch_bytes=options.get("max_batch_bytes", 1000000))


@pytest.mark.parametrize("kind", ["sales", "stock"])
def test_sorted_batches_preserve_all_fields_and_exact_values(kind):
    fix = fixture(kind)
    chunks = list(read(fix, kind))
    assert [b.num_rows for b in chunks] == [2, 1]
    assert pa.concat_tables(chunks).equals(fix[1], check_metadata=False)
    assert fix[3].closed and set(fix[3].sizes) == {2}
    assert f'FOR VERSION AS OF {fix[2][kind]["snapshot_id"]}' in fix[3].sql
    assert '"dwh-iceberg"."silver".' in fix[3].sql
    assert fix[3].sql.endswith('ORDER BY "sku_id"') and 'LIMIT' not in fix[3].sql


@pytest.mark.parametrize("field,value", [("sku_id", 1.0), ("sku_id", True), ("sku_id", None),
    ("sales_gmv", 1.5), ("sales_gmv", "10"), ("sales_gmv", Decimal("0.5")),
    ("sales_gmv_usd", float("nan")), ("sales_gmv_usd", 1),
    ("date", NOW), ("date", DAY - timedelta(days=1)), ("ingested_at", NOW),
    ("source_updated_at", None), ("source_manifest_id", "other"),
    ("source_contract_version", "old"), ("ingested_at", NOW.replace(tzinfo=None) - timedelta(seconds=1))])
def test_no_silent_cast_or_wrong_receipt(field, value):
    fix = fixture()
    fix[3].rows[0][PREP.SOURCE_FIELDS["sales"].index(field)] = value
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        list(read(fix))
    assert fix[3].closed


@pytest.mark.parametrize("failure", ["float_type", "timestamp_zone", "missing", "order", "no_metadata"])
def test_metadata_contract_checked_before_fetch(failure):
    fix = fixture()
    meta = fix[3].description
    if failure == "float_type":
        meta[5] = (meta[5][0], "double", None, None, None, None, None)
    elif failure == "timestamp_zone":
        index = PREP.SOURCE_FIELDS["sales"].index("ingested_at")
        meta[index] = (meta[index][0], "timestamp(6) with time zone", None, None, None, None, None)
    elif failure == "missing":
        meta.pop()
    elif failure == "order":
        meta[0], meta[1] = meta[1], meta[0]
    else:
        fix[3].description = None
    with pytest.raises(ValueError, match="Trino"):
        list(read(fix))
    assert fix[3].closed and fix[3].sizes == []


@pytest.mark.parametrize("failure", ["partial", "extra", "duplicate", "reverse", "interrupted"])
def test_eof_count_order_and_failure_close_cursor(failure):
    fix = fixture()
    if failure == "partial":
        fix[3].rows.pop()
    elif failure == "extra":
        row = deepcopy(fix[3].rows[-1])
        row[1] = 9
        fix[3].rows.append(row)
    elif failure == "duplicate":
        fix[3].rows[2][1] = 3
    elif failure == "reverse":
        fix[3].rows[2][1] = 2
    else:
        fix[3].fail_at = 2
    with pytest.raises((ValueError, RuntimeError)):
        list(read(fix))
    assert fix[3].closed


def test_early_consumer_close_cancels_cursor():
    fix = fixture()
    stream = read(fix)
    assert next(stream).num_rows == 2
    stream.close()
    assert fix[3].closed


@pytest.mark.parametrize("limit,value", [("max_batch_rows", 0), ("max_batch_rows", True),
                                        ("max_batch_bytes", 0), ("max_batch_bytes", 1)])
def test_limits_checked(limit, value):
    fix = fixture()
    with pytest.raises(ValueError):
        list(read(fix, **{limit: value}))
    if type(value) is not int or value != 1:
        assert fix[3].sql is None
    else:
        assert fix[3].closed


@pytest.mark.parametrize("rows", [[], [(None,)], [(True,)], [(0,)], [(1.0,)], [(3,)], [(1,), (1,)]])
def test_union_count_requires_valid_independent_result(rows):
    data = payload()
    inputs = INPUTS.day_inputs(bind(data), DAY)
    cursor = Cursor(rows, [])
    with pytest.raises(ValueError):
        READER.read_union_count(Connection(cursor), data[1], ROOT, inputs, DAY)
    assert cursor.closed


def test_union_count_uses_both_exact_snapshots():
    data = payload()
    inputs = INPUTS.day_inputs(bind(data), DAY)
    cursor = Cursor([(2,)], [])
    assert READER.read_union_count(Connection(cursor), data[1], ROOT, inputs, DAY) == 2
    assert cursor.sql.count('FOR VERSION AS OF') == 2
    assert '\nUNION\n' in cursor.sql and 'UNION ALL' not in cursor.sql
    assert cursor.closed
