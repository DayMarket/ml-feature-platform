"""Entity-local Airflow/Python runtime for dynamic-pricing gold snapshots."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger("airflow.task")

HIVE_METASTORE_URIS = (
    "thrift://hive-metastore.svc-data-hive-metastore.svc.cluster.local:9083"
)
ICEBERG_WAREHOUSE = "s3a://um-prod-data-platform-landing-layer/"
S3_ENDPOINT = "http://storage.yandexcloud.net"
S3_REGION = "ru-central1"
S3_CONNECTION_ID = "spark_ycs_connection"


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
        raise ValueError(f"Expected mapping in config: {path}")
    return config


def table_ref(config: Mapping[str, Any]) -> TableRef:
    table = config.get("table")
    if not isinstance(table, Mapping):
        raise ValueError("config.yaml must contain a table mapping")

    values = {}
    for field in ("catalog", "schema", "name"):
        value = table.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"config table.{field} must be a non-empty string")
        values[field] = value.strip()

    if "." in values["schema"] or "." in values["name"]:
        raise ValueError(
            "PyIceberg Hive identifiers require separate schema and table name "
            f"components, got schema={values['schema']!r}, name={values['name']!r}"
        )
    return TableRef(**values)


def parse_snapshot_timestamp(value: str) -> datetime:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = None

    if parsed is None:
        for timestamp_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z"):
            try:
                parsed = datetime.strptime(text, timestamp_format)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(
            f"Unsupported snapshot timestamp: {value!r}. "
            "Expected an ISO datetime with timezone or 'YYYY-MM-DD HH:MM:SS'."
        )

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def history_start_date(calculated_at: datetime, history_days: int) -> date:
    if history_days < 2:
        raise ValueError("history_days must be at least 2")
    return calculated_at.date() - timedelta(days=history_days - 1)


def history_end_date(calculated_at: datetime) -> date:
    return calculated_at.date() - timedelta(days=1)


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


def read_iceberg_date_range(table, start_date: date, end_date: date):
    from pyiceberg.expressions import EqualTo

    import pandas as pd

    frames = []
    current_date = start_date
    while current_date <= end_date:
        frame = table.scan(row_filter=EqualTo("date", current_date)).to_pandas()
        if not frame.empty:
            frames.append(frame)
        current_date += timedelta(days=1)

    if not frames:
        return pd.DataFrame(
            columns=[
                "date",
                "sku_id",
                "promotion_id",
                "discount_amount",
                "calculated_for_price",
                "created_at",
            ]
        )
    return pd.concat(frames, ignore_index=True)


def _to_arrow_for_table(table, frame):
    import pyarrow as pa

    arrow_schema = table.schema().as_arrow()
    expected = [field.name for field in arrow_schema]
    missing = [name for name in expected if name not in frame.columns]
    unexpected = [name for name in frame.columns if name not in expected]
    if missing:
        raise ValueError(f"DataFrame is missing columns required by {table.name()}: {missing}")
    if unexpected:
        logger.warning("Ignoring columns not present in %s: %s", table.name(), unexpected)
    return pa.Table.from_pandas(
        frame.loc[:, expected],
        schema=arrow_schema,
        preserve_index=False,
    )


def write_timestamp_snapshot(table, frame, calculated_at: datetime) -> None:
    from pyiceberg.expressions import EqualTo

    import pandas as pd

    if "calculated_at" not in frame.columns:
        frame = frame.copy()
        frame["calculated_at"] = calculated_at

    frame = frame.copy()
    frame["calculated_at"] = pd.to_datetime(frame["calculated_at"]).dt.tz_localize(None)
    invalid_timestamp = frame["calculated_at"].notna() & (
        frame["calculated_at"] != pd.Timestamp(calculated_at)
    )
    if invalid_timestamp.any():
        raise ValueError(
            f"Outgoing rows contain calculated_at other than {calculated_at}"
        )

    if "dynamic_discount_created_at" in frame.columns:
        frame["dynamic_discount_created_at"] = pd.to_datetime(
            frame["dynamic_discount_created_at"],
            errors="coerce",
        ).dt.tz_localize(None)

    arrow_table = _to_arrow_for_table(table, frame)
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo("calculated_at", calculated_at),
    )
    logger.info(
        "Wrote %d rows to %s for calculated_at=%s",
        arrow_table.num_rows,
        table.name(),
        calculated_at,
    )
