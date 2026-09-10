"""Соединить точные проверенные silver-срезы и атомарно записать один gold-день."""

from copy import deepcopy
from datetime import date, datetime, timezone

from .checkpoint import resume_day
from .inputs import bind_inputs, day_inputs, preflight_inputs, source_configs
from .preparation import join_batches
from .reader import read_batches, read_union_count
from .writer import require_head, write_day


def load_day(config, repo_root, catalog, connection, *, day, references, fetch_checked,
             manifest, ingested_at=None):
    """Соединением владеет caller; payload task=dq перечитывается перед commit/resume."""
    if not callable(fetch_checked):
        raise ValueError("Нужно чтение точных upstream DQ payloads")
    captured = datetime.now(timezone.utc) if ingested_at is None else ingested_at
    version = config["source"]["contract_version"]
    if (type(day) is not date or not isinstance(captured, datetime) or captured.utcoffset() is None
            or any(not isinstance(v, str) or not v.strip() for v in (manifest, version))):
        raise ValueError("Нужны DATE, manifest/version и aware capture")
    if day >= captured.astimezone(timezone.utc).date():
        raise ValueError("Observed читает только завершённые дни UTC")
    references = deepcopy(references)
    sources = source_configs(config, repo_root)
    bound = bind_inputs(sources, references, fetch_checked(references), days=[day], captured_at=captured)
    tables = preflight_inputs(config, sources, catalog, bound)
    inputs = day_inputs(bound, day)

    def verify():
        current = bind_inputs(sources, references, fetch_checked(references), days=[day], captured_at=captured)
        if current != bound:
            raise ValueError("Upstream DQ receipt сменился во время gold load")
        preflight_inputs(config, sources, catalog, bound)
        return True

    receipt = resume_day(config, catalog, day=day, inputs=inputs, manifest=manifest, version=version)
    if receipt is not None:
        verify()
        return receipt
    expected = read_union_count(connection, sources, repo_root, inputs, day)
    limits = {key: config["runtime"][key] for key in ("max_batch_rows", "max_batch_bytes")}
    streams = {kind: read_batches(connection, kind, sources[kind], repo_root, inputs[kind],
                                 tables[kind].schema().as_arrow(), day=day, **limits)
               for kind in sources}
    output = join_batches(streams["sales"], streams["stock"], tables["output"].schema().as_arrow(),
                          day=day, inputs=inputs, manifest=manifest, version=version,
                          ingested_at=captured, max_batch_rows=limits["max_batch_rows"])
    try:
        return write_day(config, catalog, output, day=day, expected_rows=expected, inputs=inputs,
                         manifest=manifest, version=version, ingested_at=captured, verify_inputs=verify)
    finally:
        output.close()
        for stream in streams.values():
            stream.close()


def require_planned_state(table, state, *, manifest, version, days):
    """После частичного успеха допускаются только собственные commits этого запроса."""
    if state is None:
        return
    if state.get("table_uuid") != str(table.metadata.table_uuid):
        raise ValueError("Gold UUID изменился после планирования")
    expected = state.get("snapshot_id")
    allowed_dates = {day.isoformat() for day in days}
    current = table.current_snapshot()
    while current is not None and current.snapshot_id != expected:
        props = current.summary.additional_properties if current.summary else {}
        if (props.get("source_manifest_id") != manifest or props.get("source_contract_version") != version
                or props.get("date") not in allowed_dates):
            raise ValueError("Gold изменился после планирования coverage: нужен новый запрос")
        parent = current.parent_snapshot_id
        if parent == expected:
            return
        current = table.snapshot_by_id(parent) if parent is not None else None
        if parent is not None and current is None:
            raise ValueError("Не доказана цепочка gold commits после планирования")
    if current is None and expected is not None:
        raise ValueError("Исходный gold snapshot плана недоступен")


def load_range(config, repo_root, catalog, connection, *, days, references, fetch_checked,
               request_id, manifest, ingested_at=None, expected_output_state=None):
    """Scheduled/manual используют один последовательный writer, DQ выполняется позже."""
    if not callable(fetch_checked):
        raise ValueError("Нужно чтение точных upstream DQ payloads")
    captured = datetime.now(timezone.utc) if ingested_at is None else ingested_at
    if (not isinstance(captured, datetime) or captured.utcoffset() is None
            or any(not isinstance(v, str) or not v.strip() for v in (request_id, manifest))
            or not isinstance(days, list) or not days or any(type(day) is not date for day in days)
            or days != sorted(set(days)) or days[-1] >= captured.astimezone(timezone.utc).date()):
        raise ValueError("Нужен непустой уникальный диапазон завершённых дней и ID/capture")
    days, references = list(days), deepcopy(references)
    sources = source_configs(config, repo_root)
    initial = bind_inputs(sources, references, fetch_checked(references), days=days, captured_at=captured)
    tables = preflight_inputs(config, sources, catalog, initial)
    target = tables["output"]
    table_uuid = str(target.metadata.table_uuid)
    head = target.current_snapshot()
    head_id = head.snapshot_id if head else None
    require_planned_state(target, expected_output_state, manifest=manifest,
                          version=config["source"]["contract_version"], days=days)

    def same_checked(refs):
        checked = fetch_checked(refs)
        if bind_inputs(sources, refs, checked, days=days, captured_at=captured) != initial:
            raise ValueError("Upstream диапазон изменился после общего preflight")
        return checked

    receipts = []
    for day in days:
        require_head(config, catalog, table_uuid, head_id)
        receipt = load_day(config, repo_root, catalog, connection, day=day, references=references,
                           fetch_checked=same_checked, manifest=manifest, ingested_at=captured)
        if receipt["table_uuid"] != table_uuid:
            raise ValueError("Gold UUID изменился внутри диапазона")
        head_id = receipt["snapshot_id"]
        require_head(config, catalog, table_uuid, head_id)
        receipts.append(receipt)
    same_checked(references)
    preflight_inputs(config, sources, catalog, initial)
    require_head(config, catalog, table_uuid, head_id)
    return {"status": "written", "request_id": request_id, "snapshot_id": head_id,
            "table_uuid": table_uuid, "dates": [day.isoformat() for day in days],
            "day_receipts": receipts}
