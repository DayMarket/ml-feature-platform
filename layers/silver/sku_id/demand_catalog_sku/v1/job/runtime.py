"""Связать полные CH captures и exact seller snapshot с атомарным SKU writer."""

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path

import pyarrow as pa
import yaml

from .inputs import bind_source, migration_schema, preflight_source, source_config, validate_reference
from .preparation import _seller_source, target_ref
from .query import capture_query
from .seller_reader import metadata_query, read_seller, source_sql, table_ref
from .source_reader import FIELDS, capture_all, read_counts, read_metadata, verify_captures
from .writer import preflight, write_catalog


def validate_arguments(config, repo_root, reference, source_manifest_id, ingested_at):
    if not isinstance(source_manifest_id, str) or not source_manifest_id.strip():
        raise ValueError("Нужен source manifest SKU")
    if not isinstance(config["source"].get("contract_version"), str) or not config["source"]["contract_version"].strip():
        raise ValueError("Нужен source contract SKU")
    captured = (datetime.now(timezone.utc) if ingested_at is None else ingested_at).replace(
        microsecond=0
    )
    if not isinstance(captured, datetime) or captured.utcoffset() is None:
        raise ValueError("Нужно aware время материализации SKU")
    limits = {key: config["runtime"].get(key) for key in ("max_batch_rows", "max_batch_bytes")}
    if any(type(value) is not int or value <= 0 for value in limits.values()):
        raise ValueError("Нужны положительные лимиты source порций")
    for kind in FIELDS:
        capture_query(config, kind)
    source, schema = source_config(config, repo_root)
    validate_reference(source, reference)
    return captured, limits, source, schema


def _preflight_services(config, repo_root, catalog, connection, target):
    entries = [(config, target.schema().as_arrow())]
    if set(config.get("feature_stats", {}).get("exclude_columns", [])) - set(target.schema().column_names):
        raise ValueError("Неизвестные feature_stats.exclude_columns")
    for relative in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        path = Path(repo_root) / relative
        service = yaml.safe_load(path.read_text(encoding="utf-8"))
        identifier = target_ref(service, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет служебной таблицы {identifier}: сначала миграции")
        actual = catalog.load_table(identifier).schema().as_arrow()
        expected = migration_schema(path.parent)
        if len(actual) != len(expected) or set(actual.names) != set(expected.names):
            raise ValueError("Схема служебной таблицы не соответствует миграции")
        for field in expected:
            found = actual.field(field.name)
            same = found.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(found.type)
            if not same or found.nullable != field.nullable:
                raise ValueError(f"Неверный тип/nullable служебной таблицы: {field.name}")
        entries.append((service, actual))
    for cfg, schema in entries:
        metadata_query(connection, f"SELECT * FROM {table_ref(cfg, repo_root)} LIMIT 0", schema)


def _rows(table):
    for batch in table.to_batches(max_chunksize=100000):
        yield from batch.to_pylist()


def execute_load(config, repo_root, *, catalog, client, connection, reference, get_checked,
                 source_manifest_id, ingested_at=None):
    """Clients принадлежат caller; get_checked читает task=dq точного seller run."""
    if not callable(get_checked):
        raise ValueError("Нужен exact seller DQ getter")
    captured, limits, seller_config, expected = validate_arguments(config, repo_root, reference, source_manifest_id, ingested_at)
    reference = deepcopy(reference)
    checked = deepcopy(get_checked(deepcopy(reference)))
    bound = bind_source(seller_config, reference, checked, captured_at=captured)
    target = preflight(config, catalog)
    initial_target = target.metadata_location
    input_table, seller_schema = preflight_source(seller_config, catalog, bound, expected)
    schema_id = input_table.snapshot_by_id(bound["receipt"]["snapshot_id"]).schema_id
    _preflight_services(config, repo_root, catalog, connection, target)
    metadata_query(connection, f"SELECT * FROM {table_ref(seller_config, repo_root)} "
                   f"FOR VERSION AS OF {bound['receipt']['snapshot_id']} LIMIT 0", seller_schema)
    metadata = read_metadata(config, client)
    counts = read_counts(config, client)
    seller = read_seller(seller_config, repo_root, connection, bound, seller_schema, **limits)
    _seller_source(seller, bound, captured)
    captures = capture_all(config, client, counts, metadata, **limits)

    def verify_seller():
        current = deepcopy(get_checked(deepcopy(reference)))
        rebound = bind_source(seller_config, reference, current, captured_at=captured)
        if current != checked or rebound != bound:
            raise ValueError("DQ payload выбранного seller run изменился")
        table, _ = preflight_source(seller_config, catalog, bound, expected)
        if table.snapshot_by_id(bound["receipt"]["snapshot_id"]).schema_id != schema_id:
            raise ValueError("Schema ID выбранного seller snapshot изменился")

    def verify_source():
        verify_seller()
        if not verify_captures(config, client, captures, counts, metadata, **limits):
            return False
        verify_seller()
        return True

    result = write_catalog(config, catalog, captures["sku"], categories=_rows(captures["category"]),
        goldens=_rows(captures["golden"]), active_links=_rows(captures["active_links"]), counts=counts,
        seller=seller, bound_seller=bound, source_manifest_id=source_manifest_id, ingested_at=captured,
        verify_source=verify_source, expected_metadata_location=initial_target)
    result["source_audit"].update(
        captures={kind: {"rows_count": counts[kind], "source_columns": [list(item) for item in metadata[kind]],
                         "query_sha256": sha256(capture_query(config, kind).encode()).hexdigest()}
                  for kind in FIELDS},
        seller={"reference": reference, "receipt": bound["receipt"], "schema_id": schema_id,
                "identifier": list(target_ref(seller_config, catalog.name)), "catalog": catalog.name,
                "query_sha256": sha256(source_sql(seller_config, repo_root, bound, seller_schema).encode()).hexdigest()},
        verified_at=datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    )
    json.dumps(result)
    return result
