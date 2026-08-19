"""Entity-local Airflow/Python runtime for the online SKU buyout projection."""

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


def parse_interval_timestamp(value: str) -> datetime:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = None

    if parsed is None:
        for timestamp_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, timestamp_format)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(
            f"Unsupported interval timestamp: {value!r}. "
            "Expected an ISO datetime with timezone or 'YYYY-MM-DD HH:MM:SS'."
        )

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def previous_utc_date(value: str) -> date:
    """Партиция равна analyze_date снапшота: дата конца интервала минус сутки."""
    return parse_interval_timestamp(value).date() - timedelta(days=1)


# ci_config.yaml маппит Iceberg-каталог платформы в каталог Trino.
TRINO_CATALOG_BY_ICEBERG_CATALOG = {"iceberg": "dwh-iceberg"}


def trino_table_name(ref: TableRef) -> str:
    """Имя таблицы платформы в Trino: "dwh-iceberg".<schema>.<name>."""
    trino_catalog = TRINO_CATALOG_BY_ICEBERG_CATALOG.get(ref.catalog)
    if not trino_catalog:
        raise ValueError(
            f"No Trino catalog mapping for Iceberg catalog {ref.catalog!r}"
        )
    return f'"{trino_catalog}".{ref.schema}.{ref.name}'


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


def require_non_empty(frame, partition_date: date) -> None:
    """Guard against replacing a good partition with an empty source read."""
    if frame.empty:
        raise RuntimeError(
            f"Source query returned no rows for date={partition_date}; "
            "refusing to overwrite the partition."
        )


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


def write_daily_snapshot(table, frame, partition_date: date) -> None:
    from pyiceberg.expressions import EqualTo

    import pandas as pd

    frame = frame.copy()
    if "date" not in frame.columns:
        frame["date"] = partition_date

    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    invalid_dates = frame["date"].notna() & (frame["date"] != partition_date)
    if invalid_dates.any():
        raise ValueError(f"Outgoing rows contain a date other than {partition_date}")

    arrow_table = _to_arrow_for_table(table, frame)
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo("date", partition_date),
    )
    logger.info(
        "Wrote %d rows to %s for date=%s",
        arrow_table.num_rows,
        table.name(),
        partition_date,
    )
