"""Entity-local Airflow/Python runtime for the search query_id dictionary."""

from __future__ import annotations

import gc
import importlib.util
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
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

OUTPUT_COLUMNS = ("updated_at", "query_text", "query_id", "version")


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
    analyzer: str
    auth: tuple[str, str] | None
    headers: dict[str, str]


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
    """Accept Airflow/Pendulum ISO timestamps with or without timezone, or `YYYY-MM-DD HH:MM:SS`."""
    text = str(value or "").strip()
    if not text:
        raise ValueError("timestamp must be provided")

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
            f"Unsupported timestamp: {value!r}. Expected an ISO datetime with or "
            "without timezone, or 'YYYY-MM-DD HH:MM:SS'."
        )

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse_partition_date(value: str) -> date:
    """Accept a bare `YYYY-MM-DD` day or any timestamp form understood by parse_snapshot_timestamp."""
    text = str(value or "").strip()
    if not text:
        raise ValueError("partition_date must be provided as YYYY-MM-DD")
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    try:
        return parse_snapshot_timestamp(text).date()
    except ValueError as exc:
        raise ValueError(
            f"Unsupported partition_date: {value!r}. Expected 'YYYY-MM-DD' or a "
            "supported timestamp form."
        ) from exc


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
    """Build the `_analyze` endpoint for the configured index from an Airflow connection."""
    from airflow.sdk import BaseHook

    conn_id = str(config["conn_id"])
    index = str(config.get("index") or "").strip()
    analyzer = str(config.get("analyzer") or "").strip()
    if not index:
        raise ValueError(
            "source.elasticsearch.index must be set in config.yaml before the DAG can run"
        )
    if not analyzer:
        raise ValueError(
            "source.elasticsearch.analyzer must be set in config.yaml before the DAG can run"
        )

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
        url=urljoin(host.rstrip("/") + "/", f"{index.strip('/')}/_analyze"),
        analyzer=analyzer,
        auth=auth,
        headers={str(key): str(value) for key, value in headers.items()},
    )


def _load_job_module(filename: str, module_name: str):
    if module_name in sys.modules:
        return sys.modules[module_name]

    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_normalize_module():
    return _load_job_module("normalize.py", "search_query_id_normalize")


def load_analyze_module():
    return _load_job_module("analyze.py", "search_query_id_analyze")


def load_stop_words_pattern(entity_dir: str | Path, stop_words_path: str):
    normalize = load_normalize_module()
    path = Path(entity_dir) / stop_words_path
    words = normalize.load_stop_words(path)
    if not words:
        logger.warning(
            "Stop-word list %s is empty; queries are only lowercased and space-collapsed",
            path,
        )
    else:
        logger.info("Loaded %d stop words from %s", len(words), path)
    return normalize.build_stop_words_pattern(words)


def extract_queries(frame) -> list[str]:
    """Drop nulls and duplicates the way the source anti-join intends, preserving Trino order."""
    if "original_query" not in frame.columns:
        raise ValueError(
            f"Trino result is missing 'original_query'; got {list(frame.columns)}"
        )

    queries = []
    seen = set()
    for value in frame["original_query"].tolist():
        if value is None:
            continue
        text = str(value)
        if not text.strip() or text in seen:
            continue
        seen.add(text)
        queries.append(text)

    logger.info("Collected %d distinct new queries from Trino", len(queries))
    return queries


def build_query_id_rows(
    queries: Sequence[str],
    stop_words_pattern,
    elastic: ElasticsearchConfig,
    parallel_jobs: int,
    timeout_seconds: int,
    retry_count: int,
    version: str,
    updated_at: datetime,
) -> list[dict[str, Any]]:
    """Clean each query, analyze it in Elasticsearch, and reduce tokens to one canonical query_id."""
    from joblib import Parallel, delayed

    if parallel_jobs < 1:
        raise ValueError("parallel_jobs must be at least 1")

    normalize = load_normalize_module()
    analyze = load_analyze_module()

    clean_queries = [
        (original_query, normalize.remove_stop_words(original_query, stop_words_pattern))
        for original_query in queries
    ]
    analyzable = [item for item in clean_queries if item[1]]
    skipped = len(clean_queries) - len(analyzable)
    if skipped:
        logger.info("Skipped %d queries that became empty after stop-word removal", skipped)

    logger.info(
        "Analyzing %d cleaned queries with parallel_jobs=%d",
        len(analyzable),
        parallel_jobs,
    )
    token_lists = Parallel(n_jobs=parallel_jobs, backend="threading")(
        delayed(analyze.analyze_query_tokens)(
            elastic.url,
            elastic.analyzer,
            clean_query,
            elastic.auth,
            elastic.headers,
            timeout_seconds,
            retry_count,
        )
        for _, clean_query in analyzable
    )

    rows = []
    empty_query_ids = 0
    for (original_query, _), tokens in zip(analyzable, token_lists):
        query_id = normalize.build_query_id(tokens)
        if not query_id:
            empty_query_ids += 1
            continue
        rows.append(
            {
                "updated_at": updated_at,
                "query_text": original_query,
                "query_id": query_id,
                "version": version,
            }
        )

    if empty_query_ids:
        logger.info("Skipped %d queries with no analyzer tokens", empty_query_ids)
    logger.info("Built %d query_id rows for version=%s", len(rows), version)
    return rows


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

            delay = min(max_sleep_seconds, initial_sleep_seconds * (2 ** (attempt - 1)))
            logger.warning(
                "Iceberg commit lock during %s, attempt %d/%d; retrying in %d seconds",
                operation_name,
                attempt,
                attempts,
                delay,
            )
            sleep_fn(delay)

    raise RuntimeError(f"Unexpected retry loop exit during {operation_name}")


def _to_arrow_for_table(table, rows: Sequence[Mapping[str, Any]], version: str):
    import pyarrow as pa

    arrow_schema = table.schema().as_arrow()
    expected = [field.name for field in arrow_schema]
    missing = [name for name in expected if name not in OUTPUT_COLUMNS]
    if missing:
        raise ValueError(
            f"Outgoing rows are missing columns required by {table.name()}: {missing}"
        )

    invalid_versions = {
        str(row["version"]) for row in rows if str(row["version"]) != version
    }
    if invalid_versions:
        raise ValueError(
            f"Outgoing rows contain a version other than {version!r}: {sorted(invalid_versions)}"
        )

    records = [{name: row.get(name) for name in expected} for row in rows]
    return pa.Table.from_pylist(records).cast(arrow_schema, safe=False)


def append_query_id_rows(
    table,
    rows: Sequence[Mapping[str, Any]],
    version: str,
    write_chunk_size: int,
) -> int:
    """Append-only write: existing (query_text, version) pairs are excluded by the source anti-join."""
    if write_chunk_size < 1:
        raise ValueError("write_chunk_size must be at least 1")

    if not rows:
        logger.info("No new queries for version=%s; nothing appended", version)
        return 0

    total_rows = 0
    for start in range(0, len(rows), write_chunk_size):
        arrow_table = _to_arrow_for_table(table, rows[start : start + write_chunk_size], version)
        try:
            _run_iceberg_commit(
                f"append {table.name()} version={version}",
                lambda chunk=arrow_table: table.append(chunk),
            )
            total_rows += arrow_table.num_rows
            logger.info(
                "Appended %d rows to %s (total_rows=%d)",
                arrow_table.num_rows,
                table.name(),
                total_rows,
            )
        finally:
            del arrow_table
            gc.collect()

    return total_rows
