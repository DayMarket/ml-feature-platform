"""Потоковая свёртка seller→SKU сохраняет денежные суммы, NULL и точные distinct."""

from datetime import timedelta
from decimal import Decimal, localcontext
from importlib import import_module

import pyarrow as pa
import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, DAY, schema
from ci_test.test_demand_seller_sales_writer import prepared, raw, shared

rollup = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_rollup")


def run(sources=None, **kwargs):
    batches = [prepared(row) for row in (shared() if sources is None else sources)]
    args = dict(day=DAY, manifest="sku-output", version="rollup-v1", ingested_at=CAPTURE,
                max_batch_rows=100, max_batch_bytes=1000000)
    args.update(kwargs)
    return pa.concat_tables(list(rollup.rollup_batches(iter(batches), schema("sales"), **args)))


def test_exact_distinct_not_summed_across_batches():
    result = run()
    assert result.num_rows == 1 and result.schema == schema("sales")
    row = result.to_pylist()[0]
    one = prepared().to_pylist()[0]
    assert row["sales_orders"] == 2
    assert row["sales_order_items"] == 3
    assert row["sales_units"] == one["sales_units"] * 3
    for name in rollup.MONEY:
        assert row[name] == one[name] * 3
        assert row[name + "_usd"] == pytest.approx(one[name + "_usd"] * 3)
    for name in ("fx_rate_date", "fx_rate_uzs_per_usd", "fx_rate_source", "fx_captured_at"):
        assert row[name] == one[name]
    assert row["source_manifest_id"] == "sku-output"
    assert row["source_contract_version"] == "rollup-v1"


def test_output_batches_preserve_sku_order_and_max_update():
    sources = shared() + [raw(sku_id=2)]
    second = sources[1].to_pylist()[0]["source_updated_at"] + timedelta(seconds=5)
    sources[1] = sources[1].set_column(sources[1].schema.get_field_index("source_updated_at"),
                                      "source_updated_at", pa.array([second], type=pa.timestamp("us", "UTC")))
    result = run(sources, max_batch_rows=1)
    assert result["sku_id"].to_pylist() == [1, 2]
    assert result["source_updated_at"][0].as_py() == second.replace(tzinfo=None)


def test_raw_precision_is_independent_of_decimal_context():
    value = Decimal("123456789012345678901234567890123456")
    sources = [raw(seller_id=sid, seller_key=f"seller:{sid}", sku_sales_orders=2, sku_sales_order_items=2,
                   sales_gmv=value, sales_gmv_fbo=value,
                   sales_gmv_usd=float(value) / 10, sales_gmv_fbo_usd=float(value) / 10) for sid in (1, 2)]
    # Подготовку сгенерированной фикстуры делаем вне малого global context.
    batches = [prepared(source) for source in sources]
    with localcontext() as ctx:
        ctx.prec = 6
        result = pa.concat_tables(list(rollup.rollup_batches(batches, schema("sales"), day=DAY,
            manifest="m", version="v", ingested_at=CAPTURE, max_batch_rows=1, max_batch_bytes=100000)))
    assert result["sales_gmv"][0].as_py() == Decimal(int(value) * 2)


def test_nullable_money_does_not_hide_missing_seller_contribution():
    sources = shared()
    sources[0] = raw(seller_id=10, seller_key="seller:10", sku_sales_orders=2, sku_sales_order_items=3,
                     sales_payment_value=None, sales_payment_value_usd=None)
    result = run(sources).to_pylist()[0]
    assert result["sales_payment_value"] is None and result["sales_payment_value_usd"] is None
    assert result["sales_gmv"] is not None


def test_unknown_fx_remains_null():
    batch = prepared()
    changes = {"fx_rate_date": None, "fx_rate_uzs_per_usd": None, "fx_rate_source": "unavailable"}
    changes.update({name + "_usd": None for name in rollup.MONEY})
    for name, value in changes.items():
        batch = batch.set_column(batch.schema.get_field_index(name), batch.schema.field(name),
                                 pa.array([value], type=batch.schema.field(name).type))
    result = pa.concat_tables(list(rollup.rollup_batches([batch], schema("sales"), day=DAY,
        manifest="m", version="v", ingested_at=CAPTURE, max_batch_rows=1, max_batch_bytes=100000)))
    assert all(result[n + "_usd"].null_count == 1 for n in rollup.MONEY)
    assert result["fx_rate_source"].to_pylist() == ["unavailable"]


@pytest.mark.parametrize("changes", [
    {"sku_sales_orders": 3}, {"sku_sales_order_items": 4},
    {"seller_key": "seller:10", "seller_id": 10},
])
def test_bad_controls_or_duplicate_between_batches(changes):
    sources = shared()
    sources[1] = raw(seller_id=2, seller_key="seller:2", sku_sales_orders=2, sku_sales_order_items=3)
    for name, value in changes.items():
        sources[1] = sources[1].set_column(sources[1].schema.get_field_index(name), sources[1].schema.field(name),
                                          pa.array([value], type=sources[1].schema.field(name).type))
    with pytest.raises(ValueError):
        run(sources)


def test_incomplete_sku_union_rejected_on_final_flush():
    with pytest.raises(ValueError, match="границ"):
        run(shared()[:1])


def test_mixed_capture_rejected():
    batches = [prepared(source) for source in shared()]
    name = "source_manifest_id"
    batches[1] = batches[1].set_column(batches[1].schema.get_field_index(name), batches[1].schema.field(name),
                                      pa.array(["other"]))
    with pytest.raises(ValueError, match="captures"):
        list(rollup.rollup_batches(batches, schema("sales"), day=DAY, manifest="m", version="v",
                                  ingested_at=CAPTURE, max_batch_rows=1, max_batch_bytes=100000))


def test_empty_day_and_oversize_rejected():
    with pytest.raises(ValueError, match="Пустой"):
        run([])
    with pytest.raises(ValueError, match="порци"):
        run(max_batch_bytes=1)


def test_decimal_overflow_never_wraps():
    value = Decimal("9" * 38)
    sources = [raw(seller_id=sid, seller_key=f"seller:{sid}", sku_sales_orders=2, sku_sales_order_items=2,
                   sales_gmv=value, sales_gmv_fbo=value,
                   sales_gmv_usd=float(value) / 10, sales_gmv_fbo_usd=float(value) / 10) for sid in (1, 2)]
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        run(sources)


def test_signed_promo_cancellation_preserves_small_usd_contribution():
    values = [Decimal("1" + "0" * 30), Decimal(1), Decimal("-1" + "0" * 30)]
    sources = [raw(seller_id=i, seller_key=f"seller:{i}", sku_sales_orders=3, sku_sales_order_items=3,
                   sales_marketplace_promo_value=value, sales_marketplace_promo_value_usd=float(value) / 10)
               for i, value in enumerate(values, 1)]
    result = run(sources).to_pylist()[0]
    assert result["sales_marketplace_promo_value"] == Decimal(1)
    assert result["sales_marketplace_promo_value_usd"] == pytest.approx(0.1)


def test_quantity_overflow_never_wraps():
    sources = [raw(seller_id=i, seller_key=f"seller:{i}", sku_sales_orders=2, sku_sales_order_items=2,
                   sales_units=2**63 - 1, sales_units_fbo=2**63 - 1) for i in (1, 2)]
    with pytest.raises((OverflowError, pa.ArrowInvalid)):
        run(sources)
