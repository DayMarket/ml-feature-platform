"""Entity-local runtime for daily Trino-source product prices."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger("airflow.task")

HIVE_METASTORE_URIS = (
    "thrift://hive-metastore.svc-data-hive-metastore.svc.cluster.local:9083"
)
ICEBERG_WAREHOUSE = "s3a://um-prod-data-platform-landing-layer/"
S3_ENDPOINT = "http://storage.yandexcloud.net"
S3_REGION = "ru-central1"
S3_CONNECTION_ID = "spark_ycs_connection"
TASHKENT_TIME_ZONE = ZoneInfo("Asia/Tashkent")

PRICE_COLUMNS = (
    "min_sell_price_eod",
    "avg_sell_price_eod",
    "max_sell_price_eod",
    "min_full_price_eod",
    "max_full_price_eod",
    "avg_active_sku_sell_price_eod",
    "min_active_sku_sell_price_eod",
    "max_active_sku_sell_price_eod",
)
SELL_PRICE_COLUMNS = (
    "min_sell_price_eod",
    "avg_sell_price_eod",
    "max_sell_price_eod",
)
FULL_PRICE_COLUMNS = (
    "min_full_price_eod",
    "max_full_price_eod",
)
ACTIVE_PRICE_COLUMNS = (
    "avg_active_sku_sell_price_eod",
    "min_active_sku_sell_price_eod",
    "max_active_sku_sell_price_eod",
)
SOURCE_METRIC_COLUMNS = (
    "source_rows",
    "source_skus",
    "source_products",
    "mapped_skus",
    "mapped_source_products",
    "mapping_mismatch_rows",
)


@dataclass(frozen=True)
class TableRef:
    catalog: str
    schema: str
    name: str

    @property
    def identifier(self) -> tuple[str, str]:
        return self.schema, self.name

    @property
    def qualified_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"


def load_config(path: str | Path) -> dict[str, Any]:
    import yaml

    with Path(path).open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    if not isinstance(config, dict):
        raise TypeError(f"Expected mapping in config: {path}")
    return config


def table_ref(config: Mapping[str, Any]) -> TableRef:
    table = config.get("table")
    if not isinstance(table, Mapping):
        raise TypeError("config.yaml must contain a table mapping")

    values = {}
    for field in ("catalog", "schema", "name"):
        value = table.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"config table.{field} must be a non-empty string")
        values[field] = value.strip()

    invalid = [field for field, value in values.items() if "." in value]
    if invalid:
        raise ValueError(
            "PyIceberg identifiers require separate catalog, schema, and table "
            f"components; dots found in: {invalid}"
        )
    return TableRef(**values)


def parse_interval_timestamp(value: str) -> datetime:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"Unsupported interval timestamp: {value!r}. "
            "Expected an ISO datetime with timezone or 'YYYY-MM-DD HH:MM:SS'."
        ) from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def previous_tashkent_date(value: str) -> date:
    interval_end = parse_interval_timestamp(value)
    return interval_end.astimezone(TASHKENT_TIME_ZONE).date() - timedelta(days=1)


def get_iceberg_catalog(ref: TableRef):
    from airflow.sdk import BaseHook
    from pyiceberg.catalog import load_catalog

    connection = BaseHook.get_connection(S3_CONNECTION_ID)
    extra = connection.extra_dejson
    return load_catalog(
        ref.catalog,
        **{
            "type": "hive",
            "uri": HIVE_METASTORE_URIS,
            "warehouse": ICEBERG_WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": extra["aws_access_key_id"],
            "s3.secret-access-key": extra["aws_secret_access_key"],
            "s3.region": S3_REGION,
            "s3.path-style-access": "true",
        },
    )


def preflight_table(catalog, ref: TableRef):
    try:
        exists = catalog.table_exists(ref.identifier)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Invalid PyIceberg identifier for {ref.qualified_name}: "
            f"{ref.identifier!r}; Hive Catalog requires exactly (schema, table)"
        ) from exc

    if not exists:
        raise RuntimeError(
            f"Iceberg table {ref.qualified_name} was not found by "
            f"{type(catalog).__name__} in namespace {ref.schema!r}. "
            "Verify catalog wiring and that CI migrations completed."
        )

    try:
        return catalog.load_table(ref.identifier)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load {ref.qualified_name} with {type(catalog).__name__} "
            f"using identifier {ref.identifier!r}"
        ) from exc


def query_trino(conn_id: str, sql: str):
    from airflow.providers.trino.hooks.trino import TrinoHook

    hook = TrinoHook(trino_conn_id=conn_id)
    frame = hook.get_pandas_df(sql)
    logger.info("Trino query returned shape=%s (conn=%s)", frame.shape, conn_id)
    return frame


def validate_source_metrics(
    metrics,
    price_date: date,
) -> dict[str, int]:
    missing = [name for name in SOURCE_METRIC_COLUMNS if name not in metrics.columns]
    if missing:
        raise ValueError(f"Source metrics are missing columns: {missing}")
    if len(metrics.index) != 1:
        raise ValueError(
            f"Expected one source-metrics row for {price_date}, got {len(metrics.index)}"
        )

    values = {name: int(metrics.iloc[0][name]) for name in SOURCE_METRIC_COLUMNS}
    if values["source_rows"] <= 0:
        raise ValueError(f"No EOD price rows found for price_date={price_date}")
    if values["source_skus"] <= 0 or values["source_products"] <= 0:
        raise ValueError(f"No valid SKU/product coverage for price_date={price_date}")
    if values["mapped_skus"] <= 0 or values["mapped_source_products"] <= 0:
        raise ValueError(f"No EOD prices could be mapped for price_date={price_date}")

    logger.info(
        "Source coverage for price_date=%s: rows=%d, skus=%d/%d, "
        "products=%d/%d, product_mapping_mismatches=%d",
        price_date,
        values["source_rows"],
        values["mapped_skus"],
        values["source_skus"],
        values["mapped_source_products"],
        values["source_products"],
        values["mapping_mismatch_rows"],
    )
    return values


def validate_product_prices(frame, price_date: date) -> None:
    import pandas as pd

    required = ("price_date", "product_id", *PRICE_COLUMNS)
    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise ValueError(f"Product prices are missing columns: {missing}")
    if frame.empty:
        raise ValueError(f"Product prices are empty for {price_date}")

    frame["price_date"] = pd.to_datetime(frame["price_date"]).dt.date
    if frame["price_date"].isna().any():
        raise ValueError("Product prices contain null price_date")
    if (frame["price_date"] != price_date).any():
        raise ValueError(f"Outgoing rows contain a price_date other than {price_date}")

    frame["product_id"] = pd.to_numeric(frame["product_id"], errors="coerce")
    if frame["product_id"].isna().any() or (frame["product_id"] <= 0).any():
        raise ValueError("Product prices contain invalid product_id")
    if frame.duplicated(subset=["price_date", "product_id"]).any():
        raise ValueError("Product prices contain duplicate primary keys")

    for column in PRICE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if (frame.loc[:, PRICE_COLUMNS] < 0).any(axis=None):
        raise ValueError("Product prices contain negative values")

    sell = frame.loc[:, SELL_PRICE_COLUMNS]
    partially_null_sell = sell.isna().any(axis=1) & ~sell.isna().all(axis=1)
    if partially_null_sell.any():
        raise ValueError(
            "Sell price columns must be either all null or all populated"
        )
    populated_sell = sell.notna().all(axis=1)
    invalid_sell_order = populated_sell & (
        (frame["min_sell_price_eod"] > frame["avg_sell_price_eod"])
        | (frame["avg_sell_price_eod"] > frame["max_sell_price_eod"])
    )
    if invalid_sell_order.any():
        raise ValueError("Sell prices violate min <= avg <= max")

    full = frame.loc[:, FULL_PRICE_COLUMNS]
    partially_null_full = full.isna().any(axis=1) & ~full.isna().all(axis=1)
    if partially_null_full.any():
        raise ValueError(
            "Full price columns must be either all null or all populated"
        )
    populated_full = full.notna().all(axis=1)
    invalid_full_order = populated_full & (
        frame["min_full_price_eod"] > frame["max_full_price_eod"]
    )
    if invalid_full_order.any():
        raise ValueError("Full prices violate min <= max")

    active = frame.loc[:, ACTIVE_PRICE_COLUMNS]
    partially_null = active.isna().any(axis=1) & ~active.isna().all(axis=1)
    if partially_null.any():
        raise ValueError(
            "Active price columns must be either all null or all populated"
        )

    populated = active.notna().all(axis=1)
    invalid_order = populated & (
        (
            frame["min_active_sku_sell_price_eod"]
            > frame["avg_active_sku_sell_price_eod"]
        )
        | (
            frame["avg_active_sku_sell_price_eod"]
            > frame["max_active_sku_sell_price_eod"]
        )
    )
    if invalid_order.any():
        raise ValueError("Active prices violate min <= avg <= max")


def _to_arrow_for_table(table, frame):
    import pyarrow as pa

    arrow_schema = table.schema().as_arrow()
    expected = [field.name for field in arrow_schema]
    missing = [name for name in expected if name not in frame.columns]
    unexpected = [name for name in frame.columns if name not in expected]
    if missing:
        raise ValueError(
            f"DataFrame is missing columns required by {table.name()}: {missing}"
        )
    if unexpected:
        logger.warning(
            "Ignoring columns not present in %s: %s", table.name(), unexpected
        )
    return pa.Table.from_pandas(
        frame.loc[:, expected],
        schema=arrow_schema,
        preserve_index=False,
    )


def write_daily_prices(
    table,
    frame,
    price_date: date,
) -> None:
    from pyiceberg.expressions import EqualTo

    frame = frame.copy()
    validate_product_prices(frame, price_date)
    arrow_table = _to_arrow_for_table(table, frame)
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo("price_date", price_date),
    )
    logger.info(
        "Wrote %d rows to %s for price_date=%s",
        arrow_table.num_rows,
        table.name(),
        price_date,
    )
