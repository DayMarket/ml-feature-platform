"""Порционная обработка и стабильные fingerprints без сетевых источников."""

from datetime import date, datetime
from decimal import Decimal
from importlib import import_module

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ci_test.test_demand_observed_preparation import join, source


@pytest.mark.parametrize("size", [1, 2, 7])
def test_join_preserves_values_with_different_chunk_boundaries(size):
    sales_ids, stock_ids = [1, 2, 8, 10, 11], [2, 3, 4, 8, 12, 13]
    amount = Decimal("12345678901234567890123456789012345678")
    sales = (source("sales", sales_ids[i:i + 2], sales_gmv=amount) for i in range(0, 5, 2))
    stock = (source("stock", stock_ids[i:i + 3]) for i in range(0, 6, 3))
    result = list(join(sales, stock, max_batch_rows=size))
    assert all(batch.num_rows <= size for batch in result)
    rows = pa.concat_tables(result).to_pylist()
    assert [row["sku_id"] for row in rows] == sorted(set(sales_ids) | set(stock_ids))
    for row in rows:
        assert row["sales_component_present"] == (row["sku_id"] in sales_ids)
        assert row["is_in_stock_eod"] == (row["sku_id"] in stock_ids)
        assert row["sales_gmv"] == (amount if row["sku_id"] in sales_ids else None)


def test_join_produces_output_without_materializing_a_day():
    consumed, closed = [], []

    def stream(kind, first):
        try:
            for sku in range(first, 100, 2):
                consumed.append(kind)
                if len(consumed) > 2:
                    raise AssertionError("Прочитано больше двух порций до первой выдачи")
                yield source(kind, [sku])
        finally:
            closed.append(kind)

    output = join(stream("sales", 1), stream("stock", 2), max_batch_rows=1)
    assert next(output)["sku_id"].to_pylist() == [1]
    output.close()
    assert sorted(closed) == ["sales", "stock"]


@pytest.mark.parametrize("entity", [
    "gold.sku_id.demand_observed_daily",
    "silver.sku_id.demand_stock_daily",
    "silver.sku_id.demand_sales_daily",
    "silver.sku_id_seller_key.demand_seller_sales_observed_daily",
    "silver.sku_id_seller_key.demand_finance_daily",
    "silver.sku_id_estimate_kind.demand_restored_daily",
])
def test_fingerprint_ignores_chunks_offsets_and_null_payloads(tmp_path, entity):
    fingerprint = import_module(f"layers.{entity}.v1.job.writer").fingerprint
    table = pa.table({
        "amount": pa.array([Decimal(0), None, Decimal("12345678901234567890123456789012345678")],
                           type=pa.decimal128(38, 0)),
        "label": pa.array(["", None, "наличие"]),
        "large_label": pa.array(["", None, "наличие"], type=pa.large_string()),
        "value": pa.array([0.0, 999.0, 1.5], mask=[False, True, False]),
        "day": pa.array([date(2026, 1, 1), None, date(2026, 1, 3)]),
        "capture": pa.array([datetime(2026, 1, 1), None, datetime(2026, 1, 3)]),
        "flag": pa.array([True, None, False]),
    })
    expected = fingerprint(table)
    assert fingerprint(pa.concat_tables([table.slice(0, 1), table.slice(1)])) == expected
    assert fingerprint(pa.concat_tables([table, table]).slice(3)) == expected
    path = tmp_path / "copy.parquet"
    pq.write_table(table, path)
    assert fingerprint(pq.read_table(path)) == expected
    altered = table.set_column(3, "value", pa.array([0.0, -999.0, 1.5], mask=[False, True, False]))
    assert fingerprint(altered) == expected
    changed = table.set_column(3, "value", pa.array([0.0, 0.0, 1.5]))
    assert fingerprint(changed) != expected
