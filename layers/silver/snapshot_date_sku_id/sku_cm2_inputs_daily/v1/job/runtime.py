"""Runtime helpers for the daily SKU CM2 input snapshot."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("airflow.task")

HIVE_METASTORE_URIS = (
    "thrift://hive-metastore.svc-data-hive-metastore.svc.cluster.local:9083"
)
ICEBERG_WAREHOUSE = "s3a://um-prod-data-platform-landing-layer/"
S3_ENDPOINT = "http://storage.yandexcloud.net"
S3_REGION = "ru-central1"
S3_CONNECTION_ID = "spark_ycs_connection"

OUTPUT_COLUMNS = (
    "snapshot_date",
    "sku_id",
    "product_id",
    "dimensional_group",
    "sell_price_uzs",
    "commission_pct",
    "n_orders_28d",
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


def parse_airflow_timestamp(value: str) -> datetime:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            f"Unsupported data_interval_end: {value!r}. "
            "Expected an ISO datetime with timezone or 'YYYY-MM-DD HH:MM:SS'."
        ) from error

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def previous_utc_date(value: datetime | str) -> date:
    parsed = parse_airflow_timestamp(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.date() - timedelta(days=1)


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
    except (TypeError, ValueError) as error:
        raise RuntimeError(
            f"Invalid PyIceberg identifier for {ref.qualified_name}: "
            f"{ref.identifier!r}; Hive Catalog requires exactly (schema, table)"
        ) from error

    if not exists:
        raise RuntimeError(
            f"Iceberg table {ref.qualified_name} was not found by "
            f"{type(catalog).__name__} in namespace {ref.schema!r}. "
            "Verify catalog wiring and that CI migrations completed."
        )

    try:
        return catalog.load_table(ref.identifier)
    except Exception as error:
        raise RuntimeError(
            f"Failed to load {ref.qualified_name} with {type(catalog).__name__} "
            f"using identifier {ref.identifier!r}"
        ) from error


def query_trino(conn_id: str, sql: str):
    from airflow.providers.trino.hooks.trino import TrinoHook

    hook = TrinoHook(trino_conn_id=conn_id)
    frame = hook.get_pandas_df(sql)
    logger.info("Trino query returned shape=%s (conn=%s)", frame.shape, conn_id)
    return frame


def _validate_nullable_numeric(frame, column: str):
    import pandas as pd

    was_present = frame[column].notna()
    converted = pd.to_numeric(frame[column], errors="coerce")
    if converted.loc[was_present].isna().any():
        raise ValueError(f"SKU CM2 snapshot contains non-numeric {column}")
    frame[column] = converted
    return converted


def validate_snapshot(
    frame,
    snapshot_date: date,
    allowed_dimensional_groups: Sequence[str],
    min_commission_pct: float,
    max_commission_pct: float,
) -> None:
    import pandas as pd

    missing = [name for name in OUTPUT_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"SKU CM2 snapshot is missing columns: {missing}")
    if frame.empty:
        raise ValueError(f"SKU CM2 snapshot is empty for {snapshot_date}")

    frame["snapshot_date"] = pd.to_datetime(
        frame["snapshot_date"],
        errors="coerce",
    ).dt.date
    if frame["snapshot_date"].isna().any():
        raise ValueError("SKU CM2 snapshot contains null snapshot_date")
    if (frame["snapshot_date"] != snapshot_date).any():
        raise ValueError(
            f"Outgoing rows contain a snapshot_date other than {snapshot_date}"
        )

    for column in ("sku_id", "product_id"):
        converted = _validate_nullable_numeric(frame, column)
        if converted.isna().any():
            raise ValueError(f"SKU CM2 snapshot contains null {column}")
        if (converted % 1 != 0).any():
            raise ValueError(f"SKU CM2 snapshot contains non-integer {column}")
        frame[column] = converted.astype("int64")

    if frame.duplicated(subset=["snapshot_date", "sku_id"]).any():
        raise ValueError("SKU CM2 snapshot contains duplicate primary keys")

    allowed_groups = set(allowed_dimensional_groups)
    if not allowed_groups:
        raise ValueError("allowed_dimensional_groups must not be empty")
    if frame["dimensional_group"].isna().any():
        raise ValueError("SKU CM2 snapshot contains null dimensional_group")
    invalid_groups = (
        set(frame["dimensional_group"].astype(str).unique()) - allowed_groups
    )
    if invalid_groups:
        raise ValueError(
            "SKU CM2 snapshot contains unsupported dimensional_group values: "
            f"{sorted(invalid_groups)}"
        )

    prices = _validate_nullable_numeric(frame, "sell_price_uzs")
    if (prices.dropna() < 0).any():
        raise ValueError("SKU CM2 snapshot contains negative sell_price_uzs")

    commissions = _validate_nullable_numeric(frame, "commission_pct")
    populated_commissions = commissions.dropna()
    if (populated_commissions < float(min_commission_pct)).any() or (
        populated_commissions > float(max_commission_pct)
    ).any():
        raise ValueError("SKU CM2 snapshot contains commission_pct outside [0, 100]")

    orders = _validate_nullable_numeric(frame, "n_orders_28d")
    if orders.isna().any():
        raise ValueError("SKU CM2 snapshot contains null n_orders_28d")
    if (orders < 0).any():
        raise ValueError("SKU CM2 snapshot contains negative n_orders_28d")
    if (orders % 1 != 0).any():
        raise ValueError("SKU CM2 snapshot contains non-integer n_orders_28d")
    frame["n_orders_28d"] = orders.astype("int64")


def log_snapshot_metrics(frame, snapshot_date: date) -> None:
    group_counts = frame["dimensional_group"].value_counts().to_dict()
    logger.info(
        "S6 coverage for snapshot_date=%s: rows=%d, price_null_share=%.6f, "
        "commission_null_share=%.6f, zero_order_share=%.6f, groups=%s",
        snapshot_date,
        len(frame.index),
        float(frame["sell_price_uzs"].isna().mean()),
        float(frame["commission_pct"].isna().mean()),
        float((frame["n_orders_28d"] == 0).mean()),
        group_counts,
    )


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
            "Ignoring columns not present in %s: %s",
            table.name(),
            unexpected,
        )
    return pa.Table.from_pandas(
        frame.loc[:, expected],
        schema=arrow_schema,
        preserve_index=False,
    )


def write_snapshot(
    *,
    table,
    frame,
    snapshot_date: date,
    allowed_dimensional_groups: Sequence[str],
    min_commission_pct: float,
    max_commission_pct: float,
) -> None:
    from pyiceberg.expressions import EqualTo

    validate_snapshot(
        frame,
        snapshot_date,
        allowed_dimensional_groups,
        min_commission_pct,
        max_commission_pct,
    )
    log_snapshot_metrics(frame, snapshot_date)
    arrow_table = _to_arrow_for_table(table, frame)
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo("snapshot_date", snapshot_date),
    )
    logger.info(
        "Wrote %d rows to %s for snapshot_date=%s",
        arrow_table.num_rows,
        table.name(),
        snapshot_date,
    )
