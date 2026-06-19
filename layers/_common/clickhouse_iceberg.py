"""Shared runtime for ClickHouse-source -> Iceberg feature-platform jobs.

This is the first non-Spark layer runtime in the repository. It is used by the
``location_forecast`` feature DAG: every task reads from ClickHouse through a
confirmed Airflow connection and writes the repository-managed output to an
Iceberg table with ``pyiceberg`` (see AGENTS.md "Trino/ClickHouse-source layer
pipelines").

The Iceberg catalog wiring mirrors the Spark layer template
(``config/spark/layer_spark_application.yaml``) one-to-one so that tables
created by Spark migrations and tables written here share the same Hive
metastore, warehouse and S3 (Yandex Cloud) endpoint. S3 credentials are read
from the same Airflow connection the Spark factory uses (``spark_ycs_connection``).

Runtime dependencies (pyiceberg, pyarrow, the ClickHouse hook) ship in the
Airflow image ``ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2``; they are not
expected to be importable in CI/local validation, so this module imports them
lazily inside functions.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger("airflow.task")

# --- Iceberg catalog contract (mirrors config/spark/layer_spark_application.yaml) ---
ICEBERG_CATALOG_NAME = "iceberg"
HIVE_METASTORE_URIS = (
    "thrift://hive-metastore.svc-data-hive-metastore.svc.cluster.local:9083"
)
ICEBERG_WAREHOUSE = "s3a://um-prod-data-platform-landing-layer/"
S3_ENDPOINT = "http://storage.yandexcloud.net"
S3_REGION = "ru-central1"
# Airflow connection that holds the warehouse S3 credentials (same one the Spark
# factory reads in layers/**/config/factory.py).
S3_CONNECTION_ID = "spark_ycs_connection"


def parse_partition_date(value: str) -> date:
    """Parse an Airflow interval boundary into a calendar date.

    Accepts Airflow/Pendulum ISO timestamps with timezone
    (``2026-06-17T00:00:00+00:00``, ``2026-06-17T00:00:00Z``,
    ``2026-06-17 00:00:00+00:00``) and the shared-template format
    ``YYYY-MM-DD HH:MM:SS`` as well as a bare ``YYYY-MM-DD``.
    """
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    for date_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue
    raise ValueError(
        f"Unsupported partition value: {value!r}. "
        "Expected ISO datetime with timezone or 'YYYY-MM-DD HH:MM:SS'."
    )


def query_clickhouse(
    conn_id: str,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Run a query through the ClickHouse Airflow connection, return a DataFrame."""
    from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

    client = ClickHouseHook(clickhouse_conn_id=conn_id).get_conn()
    frame = client.query_dataframe(sql, params=params or {})
    logger.info("ClickHouse query returned shape=%s (conn=%s)", frame.shape, conn_id)
    return frame


def get_iceberg_catalog():
    """Build a pyiceberg Hive catalog matching the Spark layer configuration."""
    from airflow.sdk import BaseHook
    from pyiceberg.catalog import load_catalog

    connection = BaseHook.get_connection(S3_CONNECTION_ID)
    extra = json.loads(connection.extra)

    return load_catalog(
        ICEBERG_CATALOG_NAME,
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


def _to_arrow_for_table(table, frame: pd.DataFrame):
    """Coerce a pandas DataFrame to the Iceberg table's Arrow schema.

    Columns are selected and ordered to match the table schema; missing feature
    columns raise so a schema drift is caught loudly rather than written as nulls.
    """
    import pyarrow as pa

    arrow_schema = table.schema().as_arrow()
    expected = [field.name for field in arrow_schema]
    missing = [name for name in expected if name not in frame.columns]
    if missing:
        raise ValueError(
            f"DataFrame is missing columns required by {table.name()}: {missing}"
        )
    ordered = frame.loc[:, expected]
    return pa.Table.from_pandas(ordered, schema=arrow_schema, preserve_index=False)


def write_daily_snapshot(
    catalog,
    table_identifier: str,
    frame: pd.DataFrame,
    partition_date: date,
) -> None:
    """Idempotently replace one ``date`` partition of an Iceberg table.

    ``table_identifier`` is ``schema.name`` (e.g. ``silver.feature_platform_...``).
    The table must already exist (created by the SQL migration applied in CI);
    we deliberately do not create it here so DDL stays owned by ``migrations/``.
    """
    from pyiceberg.expressions import EqualTo

    if "date" not in frame.columns:
        frame = frame.copy()
        frame["date"] = partition_date

    table = catalog.load_table(table_identifier)
    arrow_table = _to_arrow_for_table(table, frame)
    table.overwrite(arrow_table, overwrite_filter=EqualTo("date", partition_date))
    logger.info(
        "Wrote %d rows to iceberg.%s for date=%s",
        arrow_table.num_rows,
        table_identifier,
        partition_date,
    )


def read_iceberg_date(catalog, table_identifier: str, partition_date: date) -> pd.DataFrame:
    """Read a single ``date`` partition of an Iceberg table into a DataFrame."""
    from pyiceberg.expressions import EqualTo

    table = catalog.load_table(table_identifier)
    return table.scan(row_filter=EqualTo("date", partition_date)).to_pandas()
