"""Runtime helpers for daily account demographics."""

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
OUTPUT_COLUMNS = ("dt", "account_id", "gender", "age", "city_name", "platform")


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


def dt_from_interval_end(value: str) -> datetime:
    local_date = (
        parse_airflow_timestamp(value)
        .astimezone(TASHKENT_TIME_ZONE)
        .date()
    )
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


def validate_demographics(frame, dt: datetime) -> None:
    import pandas as pd

    missing = [name for name in OUTPUT_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"Account demographics output is missing columns: {missing}")
    if frame.empty:
        raise ValueError(f"Account demographics output is empty for {dt}")

    frame["dt"] = pd.to_datetime(
        frame["dt"],
        errors="coerce",
    )
    if frame["dt"].isna().any():
        raise ValueError("Account demographics output contains null dt")
    if (frame["dt"] != dt).any():
        raise ValueError(f"Outgoing rows contain a dt other than {dt}")

    if frame["account_id"].isna().any() or (frame["account_id"] <= 0).any():
        raise ValueError("Account demographics output contains invalid account_id")
    if frame.duplicated(subset=["dt", "account_id"]).any():
        raise ValueError("Account demographics output contains duplicate primary keys")

    invalid_genders = set(frame["gender"].dropna().astype(str)) - {
        "MALE",
        "FEMALE",
    }
    if invalid_genders:
        raise ValueError(
            "Account demographics output contains invalid gender values: "
            f"{sorted(invalid_genders)}"
        )

    age_was_present = frame["age"].notna()
    frame["age"] = pd.to_numeric(frame["age"], errors="coerce")
    if frame.loc[age_was_present, "age"].isna().any():
        raise ValueError("Account demographics output contains non-numeric age")
    populated_age = frame.loc[frame["age"].notna(), "age"]
    if (populated_age < 0).any():
        raise ValueError("Account demographics output contains negative age")
    if (populated_age % 1 != 0).any():
        raise ValueError("Account demographics output contains non-integer age")
    frame["age"] = frame["age"].astype("Int64")

    invalid_city_names = frame["city_name"].dropna().astype(str).str.strip() == ""
    if invalid_city_names.any():
        raise ValueError("Account demographics output contains empty city_name")

    invalid_platforms = set(frame["platform"].dropna().astype(str)) - {
        "IOS",
        "ANDROID",
        "WEB",
    }
    if invalid_platforms:
        raise ValueError(
            "Account demographics output contains invalid platform values: "
            f"{sorted(invalid_platforms)}"
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


def write_demographics_batches(table, frames, dt: datetime) -> None:
    from pyiceberg.expressions import EqualTo

    transaction = table.transaction()
    transaction.delete(delete_filter=EqualTo("dt", dt))
    total_rows = 0
    batch_count = 0
    null_counts = {
        column: 0 for column in ("age", "gender", "city_name", "platform")
    }
    age_min = None
    age_max = None

    for frame in frames:
        if frame.empty:
            continue

        batch_count += 1
        validate_demographics(frame, dt)
        for column in null_counts:
            null_counts[column] += int(frame[column].isna().sum())
        ages = frame["age"].dropna()
        if not ages.empty:
            batch_age_min = int(ages.min())
            batch_age_max = int(ages.max())
            age_min = (
                batch_age_min if age_min is None else min(age_min, batch_age_min)
            )
            age_max = (
                batch_age_max if age_max is None else max(age_max, batch_age_max)
            )
        arrow_table = _to_arrow_for_table(table, frame)
        batch_rows = arrow_table.num_rows
        transaction.append(arrow_table)
        total_rows += batch_rows
        logger.info(
            "Staged demographics batch=%d rows=%d total_rows=%d for dt=%s",
            batch_count,
            batch_rows,
            total_rows,
            dt,
        )
        del arrow_table
        del frame
        gc.collect()

    if total_rows == 0:
        raise ValueError(f"Account demographics output is empty for {dt}")

    transaction.commit_transaction()
    logger.info(
        "Wrote %d rows in %d batches to %s for dt=%s; "
        "age_null_share=%.6f, gender_null_share=%.6f, "
        "city_null_share=%.6f, platform_null_share=%.6f, "
        "age_min=%s, age_max=%s",
        total_rows,
        batch_count,
        table.name(),
        dt,
        null_counts["age"] / total_rows,
        null_counts["gender"] / total_rows,
        null_counts["city_name"] / total_rows,
        null_counts["platform"] / total_rows,
        age_min,
        age_max,
    )
