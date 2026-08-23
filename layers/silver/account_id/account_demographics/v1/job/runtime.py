"""Runtime helpers for daily account demographics."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
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


def dt_from_interval_end(value: str) -> date:
    return parse_airflow_timestamp(value).astimezone(TASHKENT_TIME_ZONE).date()


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


def validate_demographics(frame, dt: date) -> None:
    import pandas as pd

    missing = [name for name in OUTPUT_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"Account demographics output is missing columns: {missing}")
    if frame.empty:
        raise ValueError(f"Account demographics output is empty for {dt}")

    frame["dt"] = pd.to_datetime(
        frame["dt"],
        errors="coerce",
    ).dt.date
    if frame["dt"].isna().any():
        raise ValueError("Account demographics output contains null dt")
    if (frame["dt"] != dt).any():
        raise ValueError(f"Outgoing rows contain a dt other than {dt}")

    account_was_present = frame["account_id"].notna()
    frame["account_id"] = pd.to_numeric(frame["account_id"], errors="coerce")
    if (
        (~account_was_present).any()
        or frame.loc[account_was_present, "account_id"].isna().any()
        or (frame["account_id"] <= 0).any()
    ):
        raise ValueError("Account demographics output contains invalid account_id")
    if frame.duplicated(subset=["dt", "account_id"]).any():
        raise ValueError("Account demographics output contains duplicate primary keys")

    invalid_genders = set(frame["gender"].dropna().astype(str)) - {"M", "F"}
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


def log_demographics_metrics(frame, dt: date) -> None:
    total_rows = len(frame.index)
    age_null_share = float(frame["age"].isna().mean())
    gender_null_share = float(frame["gender"].isna().mean())
    city_null_share = float(frame["city_name"].isna().mean())
    platform_null_share = float(frame["platform"].isna().mean())
    logger.info(
        "Demographics coverage for dt=%s: rows=%d, age_null_share=%.6f, "
        "gender_null_share=%.6f, city_null_share=%.6f, "
        "platform_null_share=%.6f",
        dt,
        total_rows,
        age_null_share,
        gender_null_share,
        city_null_share,
        platform_null_share,
    )

    ages = frame["age"].dropna().astype(float)
    if ages.empty:
        logger.info("Age distribution for dt=%s is empty", dt)
        return
    logger.info(
        "Age distribution for dt=%s: min=%.0f, median=%.0f, "
        "p95=%.0f, p99=%.0f, max=%.0f",
        dt,
        ages.min(),
        ages.quantile(0.50),
        ages.quantile(0.95),
        ages.quantile(0.99),
        ages.max(),
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


def write_demographics(table, frame, dt: date) -> None:
    from pyiceberg.expressions import EqualTo

    frame = frame.copy()
    validate_demographics(frame, dt)
    log_demographics_metrics(frame, dt)
    arrow_table = _to_arrow_for_table(table, frame)
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo("dt", dt),
    )
    logger.info(
        "Wrote %d rows to %s for dt=%s",
        arrow_table.num_rows,
        table.name(),
        dt,
    )
