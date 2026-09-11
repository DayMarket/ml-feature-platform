"""Свернуть exact seller-sales snapshot и атомарно заменить полный SKU-день."""

from contextlib import closing
from copy import deepcopy
from datetime import date, datetime, timezone
from hashlib import sha256
import json

from .checkpoint import resume_day
from .seller_inputs import bind_source, day_input, preflight_source, preflight_target, source_config
from .seller_reader import read_batches, snapshot_ref, source_sql
from .seller_rollup import rollup_batches
from .writer import write_day


def read_count(connection, source, repo_root, binding, day):
    ref, scope = snapshot_ref(source, repo_root, binding, day)
    with closing(connection.cursor()) as cursor:
        cursor.execute(f'SELECT count(DISTINCT "sku_id"), count(*) FROM {ref} WHERE {scope}')
        rows = cursor.fetchmany(2)
    if (not isinstance(rows, (list, tuple)) or len(rows) != 1
            or not isinstance(rows[0], (list, tuple)) or len(rows[0]) != 2
            or any(type(v) is not int or v <= 0 for v in rows[0])):
        raise ValueError("Нет полного положительного count seller/SKU дня")
    sku_count, seller_count = rows[0]
    if seller_count != binding["day_receipt"]["rows_written"] or sku_count > seller_count:
        raise ValueError("Counts не соответствуют проверенному seller-sales дню")
    return sku_count


def load_day(config, repo_root, catalog, connection, *, day, reference, fetch_checked,
             manifest, ingested_at=None):
    """Соединением владеет caller; DQ exact run перечитывается перед commit/resume."""
    captured = datetime.now(timezone.utc) if ingested_at is None else ingested_at
    if (not callable(fetch_checked) or type(day) is not date
            or not isinstance(captured, datetime) or captured.utcoffset() is None
            or day >= captured.astimezone(timezone.utc).date()
            or not isinstance(manifest, str) or not manifest.strip()):
        raise ValueError("Нужны завершённый DATE, aware capture, manifest и exact DQ reader")
    limits = {key: config["runtime"][key] for key in ("max_batch_rows", "max_batch_bytes")}
    if any(type(v) is not int or v <= 0 for v in limits.values()):
        raise ValueError("Неверные лимиты порций")
    reference = deepcopy(reference)
    source, expected_schema = source_config(config, repo_root)
    target = preflight_target(config, catalog)
    bound = bind_source(source, reference, fetch_checked(reference), days=[day], captured_at=captured)
    table, schema = preflight_source(source, catalog, bound, expected_schema)
    binding = day_input(bound, day)
    binding["schema_id"] = table.snapshot_by_id(bound["snapshot_id"]).schema_id
    binding["query_sha256"] = sha256(source_sql(source, repo_root, binding, schema, day).encode()).hexdigest()
    signature = sha256(json.dumps(binding, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    version = config["source"]["contract_version"]

    def verify():
        current = bind_source(source, reference, fetch_checked(reference), days=[day], captured_at=captured)
        if current != bound:
            raise ValueError("DQ seller-sales сменился во время SKU load")
        preflight_source(source, catalog, bound, expected_schema)
        return True

    receipt = resume_day(config, catalog, day=day, manifest=manifest, version=version, source_binding=binding)
    if receipt is not None:
        if receipt["table_uuid"] != str(target.metadata.table_uuid):
            raise ValueError("SKU-sales UUID изменился во время resume")
        verify()
        return receipt
    expected = read_count(connection, source, repo_root, binding, day)
    stream = read_batches(connection, source, repo_root, binding, schema, day=day, **limits)
    output = rollup_batches(stream, target.schema().as_arrow(), day=day, manifest=manifest,
                           version=version, ingested_at=captured, **limits)
    try:
        return write_day(config, catalog, output, day=day, expected_rows=expected, manifest=manifest,
                         version=version, ingested_at=captured, verify_source=verify,
                         source_signature=signature, source_binding=binding,
                         expected_metadata_location=target.metadata_location)
    finally:
        output.close()
        stream.close()


def require_head(config, catalog, table_uuid, snapshot_id):
    table = preflight_target(config, catalog)
    head = table.current_snapshot()
    if (str(table.metadata.table_uuid) != table_uuid
            or (head.snapshot_id if head is not None else None) != snapshot_id):
        raise ValueError("SKU-sales изменился внутри диапазона")
    return table


def load_range(config, repo_root, catalog, connection, *, days, reference, fetch_checked,
               request_id, manifest, ingested_at=None, expected_output_state=None):
    """Последовательно свернуть exact input; готовность диапазона подтверждает DQ."""
    captured = datetime.now(timezone.utc) if ingested_at is None else ingested_at
    if (not callable(fetch_checked) or not isinstance(captured, datetime) or captured.utcoffset() is None
            or not isinstance(days, list) or not days or any(type(day) is not date for day in days)
            or days != sorted(set(days)) or days[-1] >= captured.astimezone(timezone.utc).date()
            or any(not isinstance(v, str) or not v.strip() for v in (request_id, manifest))):
        raise ValueError("Нужны даты полного диапазона, request/manifest и exact DQ reader")
    if any(type(config["runtime"].get(k)) is not int or config["runtime"][k] <= 0
           for k in ("max_batch_rows", "max_batch_bytes")):
        raise ValueError("Неверные лимиты порций")
    days, reference = list(days), deepcopy(reference)
    source, expected_schema = source_config(config, repo_root)
    target = preflight_target(config, catalog)
    require_planned_state(target, expected_output_state, manifest=manifest,
                          version=config["source"]["contract_version"], days=days)
    bound = bind_source(source, reference, fetch_checked(reference), days=days, captured_at=captured)
    preflight_source(source, catalog, bound, expected_schema)
    table_uuid = str(target.metadata.table_uuid)
    snapshot = target.current_snapshot()
    head = snapshot.snapshot_id if snapshot is not None else None
    def same_checked(ref):
        checked = fetch_checked(ref)
        if bind_source(source, ref, checked, days=days, captured_at=captured) != bound:
            raise ValueError("Seller-sales вход изменился после общего preflight")
        return checked

    receipts = []
    for day in days:
        require_head(config, catalog, table_uuid, head)
        receipt = load_day(config, repo_root, catalog, connection, day=day, reference=reference,
                           fetch_checked=same_checked, manifest=manifest, ingested_at=captured)
        if receipt["table_uuid"] != table_uuid:
            raise ValueError("SKU-sales UUID изменился внутри диапазона")
        head = receipt["snapshot_id"]
        require_head(config, catalog, table_uuid, head)
        receipts.append(receipt)
    same_checked(reference)
    preflight_source(source, catalog, bound, expected_schema)
    require_head(config, catalog, table_uuid, head)
    return {"status": "written", "request_id": request_id, "snapshot_id": head,
            "table_uuid": table_uuid, "dates": [day.isoformat() for day in days], "day_receipts": receipts}


def require_planned_state(table, state, *, manifest, version, days):
    """После coverage допускаются только собственные дневные commits того же запроса."""
    if state is None:
        return
    if state.get("table_uuid") != str(table.metadata.table_uuid):
        raise ValueError("SKU-sales UUID изменился после планирования")
    expected = state.get("snapshot_id")
    allowed_dates = {day.isoformat() for day in days}
    current = table.current_snapshot()
    while current is not None and current.snapshot_id != expected:
        props = current.summary.additional_properties if current.summary else {}
        if (props.get("source_manifest_id") != manifest or props.get("source_contract_version") != version
                or props.get("date") not in allowed_dates):
            raise ValueError("SKU-sales изменился после coverage: нужен новый запрос")
        parent = current.parent_snapshot_id
        if parent == expected:
            return
        current = table.snapshot_by_id(parent) if parent is not None else None
        if parent is not None and current is None:
            raise ValueError("Не доказана цепочка SKU-sales commits после планирования")
    if current is None and expected is not None:
        raise ValueError("Исходный snapshot плана SKU-sales недоступен")
