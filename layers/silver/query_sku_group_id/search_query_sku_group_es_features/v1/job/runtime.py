"""Entity-local Airflow/Python runtime for search ES feature snapshots."""

from __future__ import annotations

import logging
import importlib.util
import gc
import gzip
import json
import os
import sys
import time
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urljoin

logger = logging.getLogger("airflow.task")

HIVE_METASTORE_URIS = (
    "thrift://hive-metastore.svc-data-hive-metastore.svc.cluster.local:9083"
)
ICEBERG_WAREHOUSE = "s3a://um-prod-data-platform-landing-layer/"
S3_ENDPOINT = "http://storage.yandexcloud.net"
S3_REGION = "ru-central1"
S3_CONNECTION_ID = "spark_ycs_connection"
ICEBERG_COMMIT_RETRY_ATTEMPTS = 8
ICEBERG_COMMIT_RETRY_INITIAL_SECONDS = 30
ICEBERG_COMMIT_RETRY_MAX_SECONDS = 300
ICEBERG_LOCK_CHECK_MIN_WAIT_SECONDS = 2
ICEBERG_LOCK_CHECK_MAX_WAIT_SECONDS = 60
ICEBERG_LOCK_CHECK_RETRIES = 10


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


@dataclass(frozen=True)
class ElasticsearchConfig:
    url: str
    auth: tuple[str, str] | None
    headers: dict[str, str]


@dataclass(frozen=True)
class RawStorageConfig:
    conn_id: str
    bucket: str
    prefix: str
    endpoint_url: str | None
    region_name: str | None
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None = None


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


def trino_table_name(ref: TableRef) -> str:
    catalog = "dwh-iceberg" if ref.catalog == "iceberg" else ref.catalog
    return f'"{catalog}".{ref.schema}.{ref.name}'


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


def previous_utc_date(value: str) -> date:
    return parse_snapshot_timestamp(value).date() - timedelta(days=1)


def parse_partition_date(value: str) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("partition_date must be provided as YYYY-MM-DD")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Unsupported partition_date: {value!r}. Expected YYYY-MM-DD.") from exc


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
            "lock-check-min-wait-time": str(ICEBERG_LOCK_CHECK_MIN_WAIT_SECONDS),
            "lock-check-max-wait-time": str(ICEBERG_LOCK_CHECK_MAX_WAIT_SECONDS),
            "lock-check-retries": str(ICEBERG_LOCK_CHECK_RETRIES),
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


def elasticsearch_config(config: Mapping[str, Any]) -> ElasticsearchConfig:
    from airflow.sdk import BaseHook

    conn_id = str(config["conn_id"])
    endpoint_path = str(config.get("endpoint_path", "/_search"))
    connection = BaseHook.get_connection(conn_id)
    extra = connection.extra_dejson

    host = connection.host or ""
    if not host:
        raise ValueError(f"Airflow connection {conn_id} must define host")
    if not host.startswith(("http://", "https://")):
        scheme = connection.schema or "https"
        host = f"{scheme}://{host}"
    if connection.port and f":{connection.port}" not in host:
        host = f"{host}:{connection.port}"

    headers = extra.get("headers", {})
    if not isinstance(headers, dict):
        raise ValueError(f"Airflow connection {conn_id} extra.headers must be a mapping")
    auth = None
    if connection.login:
        auth = (connection.login, connection.password or "")

    return ElasticsearchConfig(
        url=urljoin(host.rstrip("/") + "/", endpoint_path.lstrip("/")),
        auth=auth,
        headers={str(key): str(value) for key, value in headers.items()},
    )


def raw_storage_config(config: Mapping[str, Any]) -> RawStorageConfig:
    from airflow.sdk import BaseHook

    conn_id = str(config["conn_id"])
    connection = BaseHook.get_connection(conn_id)
    extra = connection.extra_dejson
    bucket = (
        config.get("bucket")
        or extra.get("bucket")
        or extra.get("bucket_name")
        or connection.host
    )
    if not bucket:
        raise ValueError(
            f"Airflow connection {conn_id} must define bucket in host, "
            "extra.bucket, or raw_storage.bucket"
        )

    endpoint_url = (
        config.get("endpoint_url")
        or extra.get("endpoint_url")
        or extra.get("s3_endpoint")
        or extra.get("host")
    )
    region_name = (
        config.get("region_name")
        or extra.get("region_name")
        or extra.get("region")
        or S3_REGION
    )
    return RawStorageConfig(
        conn_id=conn_id,
        bucket=str(bucket),
        prefix=str(config["prefix"]).strip("/"),
        endpoint_url=str(endpoint_url) if endpoint_url else None,
        region_name=str(region_name) if region_name else None,
        access_key_id=(
            extra.get("aws_access_key_id")
            or extra.get("access_key")
            or connection.login
        ),
        secret_access_key=(
            extra.get("aws_secret_access_key")
            or extra.get("secret_key")
            or connection.password
        ),
        session_token=extra.get("aws_session_token"),
    )


def get_s3_client(storage: RawStorageConfig):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "Raw S3 storage requires boto3 in the Airflow runtime image. "
            f"Connection {storage.conn_id!r} cannot be used without an S3 client."
        ) from exc

    kwargs = {
        "endpoint_url": storage.endpoint_url,
        "region_name": storage.region_name,
        "aws_access_key_id": storage.access_key_id,
        "aws_secret_access_key": storage.secret_access_key,
        "aws_session_token": storage.session_token,
    }
    return boto3.client(
        "s3",
        **{key: value for key, value in kwargs.items() if value},
    )


def _safe_key_part(value: str) -> str:
    return "".join(
        char if char.isalnum() or char in ("-", "_", ".", ":", "=") else "_"
        for char in str(value)
    )


def raw_date_prefix(storage: RawStorageConfig, partition_date: date) -> str:
    return f"{storage.prefix}/raw/date={partition_date.isoformat()}"


def raw_run_prefix(storage: RawStorageConfig, partition_date: date, run_id: str) -> str:
    return f"{raw_date_prefix(storage, partition_date)}/run_id={_safe_key_part(run_id)}"


def raw_date_manifest_key(storage: RawStorageConfig, partition_date: date) -> str:
    return f"{raw_date_prefix(storage, partition_date)}/manifest.json"


def raw_date_success_key(storage: RawStorageConfig, partition_date: date) -> str:
    return f"{raw_date_prefix(storage, partition_date)}/_SUCCESS"


def _delete_s3_keys(client, bucket: str, keys: Sequence[str]) -> None:
    for start in range(0, len(keys), 1000):
        batch = [{"Key": key} for key in keys[start : start + 1000]]
        if batch:
            client.delete_objects(Bucket=bucket, Delete={"Objects": batch})


def delete_s3_prefix(client, storage: RawStorageConfig, prefix: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
        if len(keys) >= 1000:
            _delete_s3_keys(client, storage.bucket, keys)
            keys.clear()
    _delete_s3_keys(client, storage.bucket, keys)


def clear_raw_success_markers(client, storage: RawStorageConfig, partition_date: date) -> None:
    _delete_s3_keys(
        client,
        storage.bucket,
        [
            raw_date_manifest_key(storage, partition_date),
            raw_date_success_key(storage, partition_date),
        ],
    )


def _put_json(client, storage: RawStorageConfig, key: str, payload: Mapping[str, Any]) -> None:
    client.put_object(
        Bucket=storage.bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def _put_text(client, storage: RawStorageConfig, key: str, payload: str) -> None:
    client.put_object(
        Bucket=storage.bucket,
        Key=key,
        Body=payload.encode("utf-8"),
        ContentType="text/plain",
    )


def _is_s3_no_such_key_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        response = getattr(current, "response", None)
        if isinstance(response, Mapping):
            error = response.get("Error", {})
            if isinstance(error, Mapping):
                code = str(error.get("Code") or "")
                if code in {"NoSuchKey", "404", "NotFound"}:
                    return True
        if current.__class__.__name__ == "NoSuchKey":
            return True
        message = str(current)
        if "NoSuchKey" in message or "specified key does not exist" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _read_s3_json(client, storage: RawStorageConfig, key: str) -> dict[str, Any]:
    response = client.get_object(Bucket=storage.bucket, Key=key)
    response_body = response["Body"]
    try:
        body = response_body.read().decode("utf-8")
    finally:
        response_body.close()
    return json.loads(body)


def _raw_run_manifest_candidates(
    client,
    storage: RawStorageConfig,
    partition_date: date,
) -> list[dict[str, Any]]:
    prefix = f"{raw_date_prefix(storage, partition_date)}/run_id="
    paginator = client.get_paginator("list_objects_v2")
    manifests = []
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if isinstance(key, str) and key.endswith("/manifest.json"):
                manifests.append(
                    {
                        "key": key,
                        "last_modified": item.get("LastModified"),
                    }
                )
    manifests.sort(
        key=lambda item: (
            item.get("last_modified") is not None,
            str(item.get("last_modified") or ""),
            item["key"],
        )
    )
    return manifests


def _raw_part_run_id(key: str) -> str | None:
    marker = "/run_id="
    if marker not in key:
        return None
    suffix = key.split(marker, 1)[1]
    return suffix.split("/", 1)[0] if "/" in suffix else None


def _raw_part_candidates(
    client,
    storage: RawStorageConfig,
    partition_date: date,
) -> list[dict[str, Any]]:
    prefix = f"{raw_date_prefix(storage, partition_date)}/run_id="
    paginator = client.get_paginator("list_objects_v2")
    runs: dict[str, dict[str, Any]] = {}
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if (
                not isinstance(key, str)
                or "/chunk=" not in key
                or "/part-" not in key
                or not key.endswith(".jsonl.gz")
            ):
                continue
            run_id = _raw_part_run_id(key)
            if not run_id:
                continue
            run = runs.setdefault(
                run_id,
                {
                    "run_id": run_id,
                    "last_modified": None,
                    "parts": [],
                },
            )
            last_modified = item.get("LastModified")
            if str(last_modified or "") > str(run["last_modified"] or ""):
                run["last_modified"] = last_modified
            run["parts"].append(
                {
                    "key": key,
                    "bytes": item.get("Size"),
                }
            )

    candidates = list(runs.values())
    for candidate in candidates:
        candidate["parts"].sort(key=lambda part: part["key"])
    candidates.sort(
        key=lambda item: (
            item.get("last_modified") is not None,
            str(item.get("last_modified") or ""),
            item["run_id"],
        )
    )
    return candidates


def load_latest_raw_manifest(client, storage: RawStorageConfig, partition_date: date) -> dict[str, Any]:
    key = raw_date_manifest_key(storage, partition_date)
    try:
        manifest = _read_s3_json(client, storage, key)
    except Exception as exc:
        if not _is_s3_no_such_key_error(exc):
            raise
        candidates = _raw_run_manifest_candidates(client, storage, partition_date)
        if candidates:
            key = candidates[-1]["key"]
            manifest = _read_s3_json(client, storage, key)
        else:
            part_candidates = _raw_part_candidates(client, storage, partition_date)
            if not part_candidates:
                raise RuntimeError(
                    "Raw Elasticsearch data was not found for "
                    f"date={partition_date.isoformat()}. Expected {key}, a "
                    f"run-level manifest under {raw_date_prefix(storage, partition_date)}/"
                    "run_id=*/manifest.json, or raw part files under "
                    "run_id=*/chunk=*/part-*.jsonl.gz."
                ) from exc
            latest_run = part_candidates[-1]
            manifest = {
                "date": partition_date.isoformat(),
                "run_id": latest_run["run_id"],
                "run_prefix": (
                    f"{raw_date_prefix(storage, partition_date)}/"
                    f"run_id={latest_run['run_id']}"
                ),
                "parts": latest_run["parts"],
                "source": "listed_raw_parts",
            }
            logger.warning(
                "Raw Elasticsearch manifest was not found for date=%s; "
                "materializing %d listed raw parts from run_id=%s",
                partition_date,
                len(latest_run["parts"]),
                latest_run["run_id"],
            )

    if manifest.get("date") != partition_date.isoformat():
        raise ValueError(
            f"Raw manifest {key} has date={manifest.get('date')!r}, "
            f"expected {partition_date.isoformat()!r}"
        )
    if manifest.get("source") == "listed_raw_parts":
        logger.info(
            "Loaded raw Elasticsearch file list for run_id=%s parts=%d",
            manifest.get("run_id"),
            len(manifest.get("parts", [])),
        )
    else:
        logger.info("Loaded raw Elasticsearch manifest %s", key)
    return manifest


def _normalize_sku_group_ids(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, str):
        value = value.strip("[]")
        items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, Sequence):
        items = list(value)
    else:
        try:
            import numpy as np

            if isinstance(value, np.ndarray):
                items = value.tolist()
            else:
                items = [value]
        except ImportError:
            items = [value]

    ids = []
    seen = set()
    for item in items:
        try:
            sku_group_id = int(item)
        except (TypeError, ValueError):
            continue
        if sku_group_id not in seen:
            ids.append(sku_group_id)
            seen.add(sku_group_id)
    return ids


def _load_analyze_module():
    module_name = "search_es_features_analyze"
    if module_name in sys.modules:
        return sys.modules[module_name]

    path = Path(__file__).with_name("analyze.py")
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def collect_elasticsearch_features(
    query_groups,
    partition_date: date,
    elastic: ElasticsearchConfig,
    search_module,
    fields: Sequence[str],
    size: int,
    parallel_jobs: int,
    timeout_seconds: int,
    retry_count: int,
):
    import pandas as pd

    records = query_groups.to_dict("records")
    rows = _collect_elasticsearch_rows(
        records=records,
        partition_date=partition_date,
        elastic=elastic,
        search_module=search_module,
        fields=fields,
        size=size,
        parallel_jobs=parallel_jobs,
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
    )
    analyze = _load_analyze_module()
    return pd.DataFrame(rows, columns=analyze.output_columns(fields))


def _collect_elasticsearch_rows(
    records: Sequence[Mapping[str, Any]],
    partition_date: date,
    elastic: ElasticsearchConfig,
    search_module,
    fields: Sequence[str],
    size: int,
    parallel_jobs: int,
    timeout_seconds: int,
    retry_count: int,
) -> list[dict[str, Any]]:
    rows = []
    seen_keys = set()
    for row in _iter_elasticsearch_rows(
        records=records,
        partition_date=partition_date,
        elastic=elastic,
        search_module=search_module,
        fields=fields,
        size=size,
        parallel_jobs=parallel_jobs,
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
    ):
        key = (row["query"], row["sku_group_id"])
        if key in seen_keys:
            continue
        rows.append(row)
        seen_keys.add(key)
    logger.info(
        "Collected %d ES feature rows from %d Trino query groups",
        len(rows),
        len(records),
    )
    return rows


def _iter_elasticsearch_hit_records(
    records: Sequence[Mapping[str, Any]],
    elastic: ElasticsearchConfig,
    search_module,
    fields: Sequence[str],
    size: int,
    parallel_jobs: int,
    timeout_seconds: int,
    retry_count: int,
):
    from joblib import Parallel, delayed

    if parallel_jobs < 1:
        raise ValueError("parallel_jobs must be at least 1")

    tasks = []
    for source_row in records:
        query = str(source_row.get("query") or "").strip()
        sku_group_ids = _normalize_sku_group_ids(source_row.get("sku_group_ids"))
        if not query or not sku_group_ids:
            continue
        tasks.append((query, sku_group_ids))

    def fetch_query_hits(query: str, sku_group_ids: list[int]) -> list[dict[str, Any]]:
        body = search_module.build_search_body(
            query=query,
            sku_group_ids=sku_group_ids,
            fields=fields,
            size=size,
        )
        data = search_module.execute_search(
            url=elastic.url,
            body=body,
            auth=elastic.auth,
            headers=elastic.headers,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
        )
        hits = data.get("hits", {}).get("hits", [])
        return [{"query": query, "hit": hit} for hit in hits]

    logger.info(
        "Fetching Elasticsearch features for %d query groups with parallel_jobs=%d",
        len(tasks),
        parallel_jobs,
    )

    result_batches = Parallel(
        n_jobs=parallel_jobs,
        backend="threading",
        return_as="generator_unordered",
        pre_dispatch=parallel_jobs * 2,
    )(
        delayed(fetch_query_hits)(query, sku_group_ids)
        for query, sku_group_ids in tasks
    )

    for batch in result_batches:
        yield from batch


def _iter_elasticsearch_rows(
    records: Sequence[Mapping[str, Any]],
    partition_date: date,
    elastic: ElasticsearchConfig,
    search_module,
    fields: Sequence[str],
    size: int,
    parallel_jobs: int,
    timeout_seconds: int,
    retry_count: int,
):
    analyze = _load_analyze_module()

    for record in _iter_elasticsearch_hit_records(
        records=records,
        elastic=elastic,
        search_module=search_module,
        fields=fields,
        size=size,
        parallel_jobs=parallel_jobs,
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
    ):
        row = analyze.hit_to_row(
            record["hit"],
            query=record["query"],
            partition_date=partition_date,
            fields=fields,
        )
        if row["sku_group_id"] != 0:
            yield row


def _chunks(values: Sequence[Mapping[str, Any]], chunk_size: int):
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")
    for start in range(0, len(values), chunk_size):
        yield values[start : start + chunk_size]


def _iter_query_group_chunks(query_groups, chunk_size: int):
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")

    if hasattr(query_groups, "iloc"):
        for start in range(0, len(query_groups), chunk_size):
            yield query_groups.iloc[start : start + chunk_size].to_dict("records")
        return

    records = query_groups.to_dict("records")
    yield from _chunks(records, chunk_size)


class _JsonlGzipPartWriter:
    def __init__(self, client, storage: RawStorageConfig, key: str):
        self._client = client
        self._storage = storage
        self.key = key
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz")
        self._path = temp_file.name
        temp_file.close()
        self._stream = gzip.open(self._path, mode="wt", encoding="utf-8")
        self.rows = 0

    def write(self, payload: Mapping[str, Any]) -> None:
        self._stream.write(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        )
        self._stream.write("\n")
        self.rows += 1

    def close_and_upload(self) -> dict[str, Any]:
        self._stream.close()
        size_bytes = os.path.getsize(self._path)
        self._client.upload_file(
            self._path,
            self._storage.bucket,
            self.key,
            ExtraArgs={
                "ContentType": "application/x-ndjson",
                "ContentEncoding": "gzip",
            },
        )
        os.unlink(self._path)
        return {
            "key": self.key,
            "rows": self.rows,
            "bytes": size_bytes,
        }


def _raw_part_key(run_prefix: str, chunk_number: int, part_number: int) -> str:
    return (
        f"{run_prefix}/chunk={chunk_number:06d}/"
        f"part-{part_number:06d}.jsonl.gz"
    )


def write_elasticsearch_raw_to_s3(
    query_groups,
    partition_date: date,
    run_id: str,
    elastic: ElasticsearchConfig,
    search_module,
    fields: Sequence[str],
    size: int,
    parallel_jobs: int,
    chunk_size: int,
    raw_file_row_limit: int,
    timeout_seconds: int,
    retry_count: int,
    storage: RawStorageConfig,
) -> dict[str, Any]:
    if raw_file_row_limit < 1:
        raise ValueError("raw_file_row_limit must be at least 1")

    client = get_s3_client(storage)
    run_prefix = raw_run_prefix(storage, partition_date, run_id)
    delete_s3_prefix(client, storage, run_prefix + "/")
    clear_raw_success_markers(client, storage, partition_date)

    manifest: dict[str, Any] = {
        "date": partition_date.isoformat(),
        "run_id": run_id,
        "run_prefix": run_prefix,
        "storage_conn_id": storage.conn_id,
        "bucket": storage.bucket,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "chunk_size": chunk_size,
        "raw_file_row_limit": raw_file_row_limit,
        "parts": [],
        "chunks": [],
        "total_records": 0,
        "total_query_groups": 0,
    }

    for chunk_number, chunk_records in enumerate(
        _iter_query_group_chunks(query_groups, chunk_size),
        start=1,
    ):
        part_number = 0
        chunk_records_count = 0
        writer: _JsonlGzipPartWriter | None = None

        def close_part() -> None:
            nonlocal writer
            if writer is None:
                return
            part = writer.close_and_upload()
            part["chunk_number"] = chunk_number
            manifest["parts"].append(part)
            logger.info(
                "Uploaded raw ES part %s rows=%d bytes=%d",
                part["key"],
                part["rows"],
                part["bytes"],
            )
            writer = None

        def ensure_writer() -> _JsonlGzipPartWriter:
            nonlocal part_number, writer
            if writer is None or writer.rows >= raw_file_row_limit:
                close_part()
                part_number += 1
                writer = _JsonlGzipPartWriter(
                    client,
                    storage,
                    _raw_part_key(run_prefix, chunk_number, part_number),
                )
            return writer

        try:
            for record in _iter_elasticsearch_hit_records(
                records=chunk_records,
                elastic=elastic,
                search_module=search_module,
                fields=fields,
                size=size,
                parallel_jobs=parallel_jobs,
                timeout_seconds=timeout_seconds,
                retry_count=retry_count,
            ):
                ensure_writer().write(
                    {
                        "date": partition_date.isoformat(),
                        "query": record["query"],
                        "hit": record["hit"],
                    }
                )
                chunk_records_count += 1
        finally:
            close_part()

        manifest["chunks"].append(
            {
                "chunk_number": chunk_number,
                "query_groups": len(chunk_records),
                "records": chunk_records_count,
            }
        )
        manifest["total_records"] += chunk_records_count
        manifest["total_query_groups"] += len(chunk_records)
        logger.info(
            "Wrote raw ES chunk %d: query_groups=%d records=%d total_records=%d",
            chunk_number,
            len(chunk_records),
            chunk_records_count,
            manifest["total_records"],
        )

    manifest["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    run_manifest_key = f"{run_prefix}/manifest.json"
    date_manifest_key = raw_date_manifest_key(storage, partition_date)
    success_key = raw_date_success_key(storage, partition_date)
    manifest["run_manifest_key"] = run_manifest_key
    manifest["date_manifest_key"] = date_manifest_key

    _put_json(client, storage, run_manifest_key, manifest)
    _put_json(client, storage, date_manifest_key, manifest)
    _put_text(client, storage, success_key, run_manifest_key)
    logger.info(
        "Finished raw ES collection for date=%s records=%d manifest=%s",
        partition_date,
        manifest["total_records"],
        date_manifest_key,
    )
    return manifest


def _is_iceberg_lock_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        class_name = current.__class__.__name__
        message = str(current)
        if class_name == "WaitingForLockException":
            return True
        if class_name == "CommitFailedException" and "lock" in message.lower():
            return True
        if "Failed to acquire lock" in message or "Wait on lock" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _run_iceberg_commit(
    operation_name: str,
    operation: Callable[[], Any],
    *,
    attempts: int = ICEBERG_COMMIT_RETRY_ATTEMPTS,
    initial_sleep_seconds: int = ICEBERG_COMMIT_RETRY_INITIAL_SECONDS,
    max_sleep_seconds: int = ICEBERG_COMMIT_RETRY_MAX_SECONDS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Any:
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if not _is_iceberg_lock_error(exc) or attempt == attempts:
                raise

            delay = min(
                max_sleep_seconds,
                initial_sleep_seconds * (2 ** (attempt - 1)),
            )
            logger.warning(
                "Iceberg commit lock during %s, attempt %d/%d; retrying in %d seconds",
                operation_name,
                attempt,
                attempts,
                delay,
            )
            sleep_fn(delay)

    raise RuntimeError(f"Unexpected retry loop exit during {operation_name}")


def stage_clear_daily_snapshot(transaction, table, partition_date: date) -> None:
    from pyiceberg.expressions import EqualTo

    transaction.delete(delete_filter=EqualTo("date", partition_date))
    logger.info("Staged clear for %s date=%s before chunked append", table.name(), partition_date)


def clear_daily_snapshot(table, partition_date: date) -> None:
    from pyiceberg.expressions import EqualTo

    import pyarrow as pa

    empty_table = pa.Table.from_pylist([], schema=table.schema().as_arrow())
    _run_iceberg_commit(
        f"clear {table.name()} date={partition_date}",
        lambda: table.overwrite(
            empty_table,
            overwrite_filter=EqualTo("date", partition_date),
        ),
    )
    logger.info("Cleared %s for date=%s before raw parse append", table.name(), partition_date)


def append_daily_chunk(transaction, table, frame, partition_date: date) -> int:
    if frame.empty:
        return 0

    frame = _prepare_daily_frame(frame, partition_date)
    arrow_table = _to_arrow_for_table(table, frame)
    written_rows = arrow_table.num_rows
    transaction.append(arrow_table)
    del arrow_table
    del frame
    gc.collect()
    logger.info(
        "Staged append of %d rows to %s for date=%s",
        written_rows,
        table.name(),
        partition_date,
    )
    return written_rows


def append_committed_daily_chunk(table, frame, partition_date: date) -> int:
    if frame.empty:
        return 0

    frame = _prepare_daily_frame(frame, partition_date)
    arrow_table = _to_arrow_for_table(table, frame)
    written_rows = arrow_table.num_rows
    _run_iceberg_commit(
        f"append {table.name()} date={partition_date}",
        lambda: table.append(arrow_table),
    )
    del arrow_table
    del frame
    gc.collect()
    logger.info(
        "Committed append of %d rows to %s for date=%s",
        written_rows,
        table.name(),
        partition_date,
    )
    return written_rows


def _append_row_buffer(
    transaction,
    table,
    rows: Sequence[Mapping[str, Any]],
    partition_date: date,
    columns: Sequence[str],
) -> int:
    import pandas as pd

    if not rows:
        return 0
    frame = pd.DataFrame(rows, columns=columns)
    return append_daily_chunk(transaction, table, frame, partition_date)


def _append_committed_row_buffer(
    table,
    rows: Sequence[Mapping[str, Any]],
    partition_date: date,
    columns: Sequence[str],
) -> int:
    import pandas as pd

    if not rows:
        return 0
    frame = pd.DataFrame(rows, columns=columns)
    return append_committed_daily_chunk(table, frame, partition_date)


def iter_raw_manifest_records(client, storage: RawStorageConfig, manifest: Mapping[str, Any]):
    for part in manifest.get("parts", []):
        key = part["key"]
        response = client.get_object(Bucket=storage.bucket, Key=key)
        body = response["Body"]
        try:
            with gzip.GzipFile(fileobj=body, mode="rb") as stream:
                for line in stream:
                    if line.strip():
                        yield json.loads(line.decode("utf-8"))
        finally:
            body.close()


def write_raw_features_to_iceberg(
    table,
    partition_date: date,
    storage: RawStorageConfig,
    fields: Sequence[str],
    write_chunk_size: int,
) -> None:
    if write_chunk_size < 1:
        raise ValueError("write_chunk_size must be at least 1")

    client = get_s3_client(storage)
    manifest = load_latest_raw_manifest(client, storage, partition_date)
    analyze = _load_analyze_module()
    output_columns = analyze.output_columns(fields)

    clear_daily_snapshot(table, partition_date)

    row_buffer = []
    total_rows = 0
    raw_records = 0
    for raw_record in iter_raw_manifest_records(client, storage, manifest):
        raw_records += 1
        query = str(raw_record.get("query") or "").strip()
        hit = raw_record.get("hit")
        if not query or not isinstance(hit, Mapping):
            continue
        row = analyze.hit_to_row(
            hit,
            query=query,
            partition_date=partition_date,
            fields=fields,
        )
        if row["sku_group_id"] == 0:
            continue
        row_buffer.append(row)

        if len(row_buffer) >= write_chunk_size:
            total_rows += _append_committed_row_buffer(
                table,
                row_buffer,
                partition_date,
                output_columns,
            )
            row_buffer.clear()

    if row_buffer:
        total_rows += _append_committed_row_buffer(
            table,
            row_buffer,
            partition_date,
            output_columns,
        )
        row_buffer.clear()

    logger.info(
        "Finished raw parse to Iceberg for %s date=%s raw_records=%d rows=%d",
        table.name(),
        partition_date,
        raw_records,
        total_rows,
    )


def write_elasticsearch_features_by_chunks(
    table,
    query_groups,
    partition_date: date,
    elastic: ElasticsearchConfig,
    search_module,
    fields: Sequence[str],
    size: int,
    parallel_jobs: int,
    chunk_size: int,
    write_chunk_size: int,
    timeout_seconds: int,
    retry_count: int,
) -> None:
    analyze = _load_analyze_module()
    output_columns = analyze.output_columns(fields)
    if write_chunk_size < 1:
        raise ValueError("write_chunk_size must be at least 1")

    total_rows = 0
    transaction = table.transaction()
    partition_clear_staged = False

    for chunk_number, chunk_records in enumerate(
        _iter_query_group_chunks(query_groups, chunk_size),
        start=1,
    ):
        rows_seen = 0
        row_buffer = []
        seen_keys = set()
        for row in _iter_elasticsearch_rows(
            records=chunk_records,
            partition_date=partition_date,
            elastic=elastic,
            search_module=search_module,
            fields=fields,
            size=size,
            parallel_jobs=parallel_jobs,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
        ):
            rows_seen += 1
            key = (row["query"], row["sku_group_id"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            row_buffer.append(row)

            if len(row_buffer) >= write_chunk_size:
                if not partition_clear_staged:
                    stage_clear_daily_snapshot(transaction, table, partition_date)
                    partition_clear_staged = True
                total_rows += _append_row_buffer(
                    transaction,
                    table,
                    row_buffer,
                    partition_date,
                    output_columns,
                )
                row_buffer.clear()

        if row_buffer:
            if not partition_clear_staged:
                stage_clear_daily_snapshot(transaction, table, partition_date)
                partition_clear_staged = True
            total_rows += _append_row_buffer(
                transaction,
                table,
                row_buffer,
                partition_date,
                output_columns,
            )
            row_buffer.clear()
        logger.info(
            "Processed ES chunk %d: query_groups=%d rows=%d total_written=%d",
            chunk_number,
            len(chunk_records),
            rows_seen,
            total_rows,
        )
        del seen_keys

    if not partition_clear_staged:
        stage_clear_daily_snapshot(transaction, table, partition_date)

    _run_iceberg_commit(
        f"commit chunked write {table.name()} date={partition_date}",
        transaction.commit_transaction,
    )

    logger.info(
        "Finished chunked ES collection for %s date=%s rows=%d",
        table.name(),
        partition_date,
        total_rows,
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


def _prepare_daily_frame(frame, partition_date: date):
    import pandas as pd

    if "date" not in frame.columns:
        frame = frame.copy()
        frame["date"] = partition_date

    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    invalid_dates = frame["date"].notna() & (frame["date"] != partition_date)
    if invalid_dates.any():
        raise ValueError(f"Outgoing rows contain a date other than {partition_date}")
    return frame


def write_daily_snapshot(table, frame, partition_date: date) -> None:
    from pyiceberg.expressions import EqualTo

    frame = _prepare_daily_frame(frame, partition_date)

    arrow_table = _to_arrow_for_table(table, frame)
    _run_iceberg_commit(
        f"overwrite {table.name()} date={partition_date}",
        lambda: table.overwrite(
            arrow_table,
            overwrite_filter=EqualTo("date", partition_date),
        ),
    )
    logger.info(
        "Wrote %d rows to %s for date=%s",
        arrow_table.num_rows,
        table.name(),
        partition_date,
    )
