"""Продолжить gold-день только после полного read-back и совпадения входных версий."""

from datetime import date, datetime
import json
import re

from .preparation import metadata, target_ref, validate_schema
from .writer import PROOF_KEY, fingerprint, input_signature, require_head


class ReadBackMismatch(RuntimeError):
    """Текущий gold-день не совпал с содержимым собственного commit."""


def verify_proof(table, proof, snapshot_id):
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

    validate_schema(table.schema().as_arrow())
    if proof["table_uuid"] != str(table.metadata.table_uuid):
        raise ValueError("Gold checkpoint другой таблицы")
    day = date.fromisoformat(proof["date"])
    metadata(proof["inputs"], proof["source_manifest_id"], proof["source_contract_version"],
             datetime.fromisoformat(proof["ingested_at"]))
    if (type(proof["rows_written"]) is not int or proof["rows_written"] <= 0
            or proof["input_signature"] != input_signature(proof["inputs"])):
        raise ValueError("Неверные count/input signature checkpoint")
    previous, total = None, 0
    for first, last, size, digest in proof["batches"]:
        if (any(type(v) is not int for v in (first, last, size)) or first <= 0 or size <= 0
                or last < first or (previous is not None and first <= previous)
                or not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest)):
            raise ValueError("Неверные границы gold checkpoint")
        previous, total = last, total + size
    if total != proof["rows_written"]:
        raise ValueError("Неполный gold checkpoint")
    scope = EqualTo("date", day)
    if table.scan(snapshot_id=snapshot_id, row_filter=scope).count() != total:
        raise ReadBackMismatch("Read-back count gold не совпал")
    for first, last, size, digest in proof["batches"]:
        part = And(scope, GreaterThanOrEqual("sku_id", first), LessThanOrEqual("sku_id", last))
        actual = table.scan(snapshot_id=snapshot_id, row_filter=part, limit=size + 1).to_arrow()
        actual = actual.sort_by([("sku_id", "ascending")])
        if actual.num_rows != size or fingerprint(actual) != digest:
            raise ReadBackMismatch("Read-back gold не совпал с исходной порцией")


def written_receipt(proof, snapshot_id, *, resumed=False):
    return {k: v for k, v in proof.items() if k != "batches"} | {
        "status": "written", "snapshot_id": snapshot_id, "resumed": resumed}


def resume_day(config, catalog, *, day, inputs, manifest, version):
    """Смена inputs требует новой записи даже при прежнем count; источник проверяет caller."""
    if type(day) is not date or not manifest or not version:
        raise ValueError("Нужны день, manifest и версия")
    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
    table = catalog.load_table(identifier)
    validate_schema(table.schema().as_arrow())
    head = table.current_snapshot()
    current = head
    while current is not None:
        properties = current.summary.additional_properties if current.summary else {}
        if PROOF_KEY in properties:
            proof = json.loads(properties[PROOF_KEY])
            if proof["date"] == day.isoformat():
                if (proof["source_manifest_id"] != manifest or proof["source_contract_version"] != version
                        or proof["input_signature"] != input_signature(inputs)):
                    return None
                try:
                    verify_proof(table, proof, head.snapshot_id)
                except ReadBackMismatch:
                    return None
                require_head(config, catalog, proof["table_uuid"], head.snapshot_id)
                return written_receipt(proof, head.snapshot_id, resumed=True)
        current = table.snapshot_by_id(current.parent_snapshot_id) if current.parent_snapshot_id else None
    return None
