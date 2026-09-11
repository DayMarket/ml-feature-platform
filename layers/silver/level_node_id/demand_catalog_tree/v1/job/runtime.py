"""Построить и записать дерево из одного полного SKU snapshot с повторным DQ binding."""

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import yaml

from .inputs import bind_source, migration_schema, preflight_source, source_config, validate_reference, validate_schema
from .preparation import prepare_tree, target_ref
from .reader import metadata_query, read_batches, source_sql, table_ref
from .writer import preflight, write_prepared


def validate_arguments(config, repo_root, reference, source_manifest_id, ingested_at):
    if not isinstance(source_manifest_id, str) or not source_manifest_id.strip():
        raise ValueError("Нужен manifest дерева")
    captured = (datetime.now(timezone.utc) if ingested_at is None else ingested_at).replace(
        microsecond=0
    )
    if not isinstance(captured, datetime) or captured.utcoffset() is None:
        raise ValueError("Нужно aware время материализации дерева")
    limits = {key: config["runtime"].get(key) for key in ("max_batch_rows", "max_batch_bytes")}
    if any(type(value) is not int or value <= 0 for value in limits.values()):
        raise ValueError("Нужны положительные лимиты порций")
    source, expected = source_config(config, repo_root)
    validate_reference(source, reference)
    return captured, limits, source, expected


def execute_load(config, repo_root, *, catalog, connection, reference, get_checked,
                 source_manifest_id, ingested_at=None):
    """Соединения принадлежат caller; get_checked повторно читает task=dq выбранного run."""
    if not callable(get_checked):
        raise ValueError("Нужен exact DQ getter")
    captured, limits, source, expected = validate_arguments(config, repo_root, reference, source_manifest_id, ingested_at)
    reference = deepcopy(reference)
    checked = deepcopy(get_checked(deepcopy(reference)))
    bound = bind_source(source, reference, checked, captured_at=captured)
    target = preflight(config, catalog)
    if set(config.get("feature_stats", {}).get("exclude_columns", [])) - set(target.schema().column_names):
        raise ValueError("Неизвестные feature_stats.exclude_columns")
    initial_target = target.metadata_location
    input_table, schema = preflight_source(source, catalog, bound, expected)
    schema_id = input_table.snapshot_by_id(bound["receipt"]["snapshot_id"]).schema_id
    # Проверяем все существующие таблицы до большого скана SKU.
    entries = [(config, target.schema().as_arrow())]
    for relative in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        path = Path(repo_root) / relative
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        identifier = target_ref(cfg, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет служебной таблицы {identifier}: сначала миграции")
        table = catalog.load_table(identifier)
        validate_schema(table.schema().as_arrow(), migration_schema(path.parent))
        entries.append((cfg, table.schema().as_arrow()))
    for cfg, arrow in entries:
        metadata_query(connection, f"SELECT * FROM {table_ref(cfg, repo_root)} LIMIT 0", arrow)
    metadata_query(connection, f"SELECT * FROM {table_ref(source, repo_root)} "
                   f"FOR VERSION AS OF {bound['receipt']['snapshot_id']} LIMIT 0", schema)

    def verify_source():
        current = deepcopy(get_checked(deepcopy(reference)))
        rebound = bind_source(source, reference, current, captured_at=captured)
        if current != checked or rebound != bound:
            raise ValueError("DQ payload выбранного SKU run изменился")
        table, _ = preflight_source(source, catalog, bound, expected)
        if table.snapshot_by_id(bound["receipt"]["snapshot_id"]).schema_id != schema_id:
            raise ValueError("Схема выбранного SKU snapshot изменилась")
        return True

    receipt = bound["receipt"]
    stream = read_batches(connection, source, repo_root, bound, schema, **limits)
    try:
        batch, audit = prepare_tree(stream, target.schema().as_arrow(), capture_date=bound["date"],
                                    catalog_version=receipt["catalog_version"], source_snapshot_id=receipt["snapshot_id"],
                                    expected_source_rows=receipt["rows_written"], source_manifest_id=source_manifest_id,
                                    source_contract_version=config["source"]["contract_version"], ingested_at=captured)
    finally:
        stream.close()
    result = write_prepared(config, catalog, batch, capture_date=bound["date"], catalog_version=receipt["catalog_version"],
                            source_snapshot_id=receipt["snapshot_id"], expected_nodes=audit["n_nodes"],
                            verify_source=verify_source, expected_metadata_location=initial_target)
    result["source_audit"] = {**audit, "reference": reference, "receipt": receipt, "schema_id": schema_id,
                              "identifier": list(target_ref(source, catalog.name)), "catalog": catalog.name,
                              "query_sha256": sha256(source_sql(source, repo_root, bound).encode()).hexdigest()}
    return result
