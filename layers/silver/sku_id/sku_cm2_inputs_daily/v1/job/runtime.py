"""Runtime helpers for daily SKU CM2 inputs."""

from __future__ import annotations

import gc
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, time, timezone
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
ALLOWED_DIMENSIONAL_GROUPS = {"SMALL", "MEDIUM", "LARGE"}

OUTPUT_COLUMNS = (
    "dt",
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


def tashkent_dt(value: datetime | str) -> datetime:
    parsed = parse_airflow_timestamp(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    local_date = parsed.astimezone(TASHKENT_TIME_ZONE).date()
    return datetime.combine(local_date, time.min)


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


def iter_trino_batches(conn_id: str, sql: str, batch_size: int):
    from airflow.providers.trino.hooks.trino import TrinoHook
    import pandas as pd

    if batch_size <= 0:
        raise ValueError("Trino query batch_size must be positive")

    hook = TrinoHook(trino_conn_id=conn_id)
    connection = hook.get_conn()
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        columns = [description[0] for description in cursor.description]
        batch_number = 0
        while rows := cursor.fetchmany(batch_size):
            batch_number += 1
            frame = pd.DataFrame.from_records(rows, columns=columns)
            logger.info(
                "Trino query returned batch=%d rows=%d (conn=%s)",
                batch_number,
                len(frame.index),
                conn_id,
            )
            yield frame
    finally:
        cursor.close()
        connection.close()


def _validate_nullable_numeric(frame, column: str):
    import pandas as pd

    was_present = frame[column].notna()
    converted = pd.to_numeric(frame[column], errors="coerce")
    if converted.loc[was_present].isna().any():
        raise ValueError(f"S6 output contains non-numeric {column}")
    frame[column] = converted
    return converted


def validate_inputs(frame, dt: datetime) -> None:
    import pandas as pd

    missing = [name for name in OUTPUT_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"S6 output is missing columns: {missing}")
    if frame.empty:
        raise ValueError(f"S6 output is empty for {dt}")

    frame["dt"] = pd.to_datetime(frame["dt"], errors="coerce")
    if frame["dt"].isna().any():
        raise ValueError("S6 output contains null dt")
    if (frame["dt"] != dt).any():
        raise ValueError(f"Outgoing rows contain a dt other than {dt}")

    for column, dtype in (("sku_id", "int64"), ("product_id", "int32")):
        converted = _validate_nullable_numeric(frame, column)
        if converted.isna().any():
            raise ValueError(f"S6 output contains null {column}")
        if (converted % 1 != 0).any():
            raise ValueError(f"S6 output contains non-integer {column}")
        frame[column] = converted.astype(dtype)

    if frame.duplicated(subset=["dt", "sku_id"]).any():
        raise ValueError("S6 output contains duplicate primary keys")

    if frame["dimensional_group"].isna().any():
        raise ValueError("S6 output contains null dimensional_group")
    invalid_groups = (
        set(frame["dimensional_group"].astype(str).unique())
        - ALLOWED_DIMENSIONAL_GROUPS
    )
    if invalid_groups:
        raise ValueError(
            "S6 output contains unsupported dimensional_group values: "
            f"{sorted(invalid_groups)}"
        )

    prices = _validate_nullable_numeric(frame, "sell_price_uzs")
    if (prices.dropna() < 0).any():
        raise ValueError("S6 output contains negative sell_price_uzs")

    commissions = _validate_nullable_numeric(frame, "commission_pct")
    populated_commissions = commissions.dropna()
    if (populated_commissions < 0).any() or (populated_commissions > 100).any():
        raise ValueError("S6 output contains commission_pct outside [0, 100]")

    orders = _validate_nullable_numeric(frame, "n_orders_28d")
    if orders.isna().any():
        raise ValueError("S6 output contains null n_orders_28d")
    if (orders < 0).any():
        raise ValueError("S6 output contains negative n_orders_28d")
    if (orders % 1 != 0).any():
        raise ValueError("S6 output contains non-integer n_orders_28d")
    frame["n_orders_28d"] = orders.astype("int64")


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


def write_input_batches(*, table, frames, dt: datetime) -> None:
    from pyiceberg.expressions import EqualTo

    transaction = table.transaction()
    transaction.delete(delete_filter=EqualTo("dt", dt))
    total_rows = 0
    batch_count = 0
    price_null_count = 0
    commission_null_count = 0
    zero_order_count = 0
    group_counts: dict[str, int] = {}

    for frame in frames:
        if frame.empty:
            continue

        batch_count += 1
        validate_inputs(frame, dt)
        price_null_count += int(frame["sell_price_uzs"].isna().sum())
        commission_null_count += int(frame["commission_pct"].isna().sum())
        zero_order_count += int((frame["n_orders_28d"] == 0).sum())
        for group, count in frame["dimensional_group"].value_counts().items():
            group_name = str(group)
            group_counts[group_name] = group_counts.get(group_name, 0) + int(
                count
            )

        arrow_table = _to_arrow_for_table(table, frame)
        batch_rows = arrow_table.num_rows
        transaction.append(arrow_table)
        total_rows += batch_rows
        logger.info(
            "Staged S6 batch=%d rows=%d total_rows=%d for dt=%s",
            batch_count,
            batch_rows,
            total_rows,
            dt,
        )
        del arrow_table
        del frame
        gc.collect()

    if total_rows == 0:
        raise ValueError(f"S6 output is empty for {dt}")

    transaction.commit_transaction()
    logger.info(
        "Wrote %d rows in %d batches to %s for dt=%s; "
        "price_null_share=%.6f, commission_null_share=%.6f, "
        "zero_order_share=%.6f, groups=%s",
        total_rows,
        batch_count,
        table.name(),
        dt,
        price_null_count / total_rows,
        commission_null_count / total_rows,
        zero_order_count / total_rows,
        group_counts,
    )
