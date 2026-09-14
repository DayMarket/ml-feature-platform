"""Сравнение capture timestamp с фактическим Arrow-типом Iceberg."""

from datetime import datetime, timezone
from importlib import import_module

import pyarrow as pa
import pytest


WRITERS = [
    "layers.silver.sku_id.demand_stock_daily.v1.job.writer",
    "layers.silver.sku_id.demand_sales_daily.v1.job.writer",
    "layers.silver.sku_id_seller_key.demand_finance_daily.v1.job.writer",
    "layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily.v1.job.writer",
    "layers.silver.sku_id_estimate_kind.demand_restored_daily.v1.job.writer",
    "layers.gold.sku_id.demand_observed_daily.v1.job.writer",
]
CAPTURE = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("module_name", WRITERS)
@pytest.mark.parametrize("timestamp_type", [pa.timestamp("us"), pa.timestamp("us", "UTC")])
def test_single_capture_accepts_both_iceberg_timestamp_representations(module_name, timestamp_type):
    writer = import_module(module_name)
    column = pa.chunked_array([[CAPTURE, CAPTURE]], type=timestamp_type)

    assert writer.single_typed_value(column, CAPTURE.replace(tzinfo=None))


@pytest.mark.parametrize("module_name", WRITERS)
def test_mixed_capture_is_still_rejected(module_name):
    writer = import_module(module_name)
    column = pa.chunked_array(
        [[CAPTURE, CAPTURE.replace(minute=1)]],
        type=pa.timestamp("us", "UTC"),
    )

    assert not writer.single_typed_value(column, CAPTURE.replace(tzinfo=None))
