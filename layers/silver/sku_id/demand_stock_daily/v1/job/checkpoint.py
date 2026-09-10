"""Подтвердить дневной commit по его metadata и полному read-back без Iceberg tags."""

from datetime import date, datetime
import json

from .preparation import target_ref, utc_naive, validate_schema

PROOF_KEY = 'demand-stock-checkpoint-v1'


class ReadBackMismatch(RuntimeError):
    """Текущий день не совпадает с содержимым commit."""


def verify_proof(table, proof, snapshot_id):
    """Проверить весь день ограниченными порциями; metadata не заменяет сверку данных."""
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
    from .writer import fingerprint

    validate_schema(table.schema().as_arrow())
    if proof['table_uuid'] != str(table.metadata.table_uuid):
        raise ValueError('Checkpoint другой таблицы')
    day = date.fromisoformat(proof['date'])
    utc_naive(datetime.fromisoformat(proof['ingested_at']))
    if (type(proof['rows_written']) is not int or proof['rows_written'] <= 0
            or not proof['source_manifest_id'] or not proof['source_contract_version']):
        raise ValueError('Неверный checkpoint')
    previous, total = None, 0
    for first, last, size, digest in proof['batches']:
        if (any(type(v) is not int for v in (first, last, size)) or size <= 0 or first <= 0
                or last < first or (previous is not None and first <= previous)
                or not isinstance(digest, str) or len(digest) != 64):
            raise ValueError('Неверные границы checkpoint')
        previous, total = last, total + size
    if total != proof['rows_written']:
        raise ValueError('Неполный checkpoint')
    scope = EqualTo('date', day)
    if table.scan(snapshot_id=snapshot_id, row_filter=scope).count() != total:
        raise ReadBackMismatch('Read-back count не совпал')
    for first, last, size, digest in proof['batches']:
        part = And(scope, GreaterThanOrEqual('sku_id', first), LessThanOrEqual('sku_id', last))
        actual = table.scan(snapshot_id=snapshot_id, row_filter=part, limit=size + 1).to_arrow()
        actual = actual.sort_by([('sku_id', 'ascending')])
        if actual.num_rows != size or fingerprint(actual) != digest:
            raise ReadBackMismatch('Read-back stock не совпал с исходной порцией')


def written_receipt(proof, snapshot_id, *, resumed=False):
    return {key: value for key, value in proof.items() if key != 'batches'} | {
        'status': 'written', 'snapshot_id': snapshot_id, 'resumed': resumed,
    }


def resume_day(config, catalog, *, day, manifest, version):
    """Повторить read-back commit того же запроса. Отсутствие proof требует перезаписи."""
    if type(day) is not date or not manifest or not version:
        raise ValueError('Нужны день, manifest и версия')
    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f'Нет таблицы {identifier}: сначала миграции')
    table = catalog.load_table(identifier)
    validate_schema(table.schema().as_arrow())
    head = table.current_snapshot()
    current = head
    while current is not None:
        properties = current.summary.additional_properties if current.summary is not None else {}
        if PROOF_KEY in properties:
            proof = json.loads(properties[PROOF_KEY])
            if proof['date'] == day.isoformat():
                if proof['source_manifest_id'] != manifest or proof['source_contract_version'] != version:
                    return None
                try:
                    verify_proof(table, proof, head.snapshot_id)
                except ReadBackMismatch:
                    return None
                table.refresh()
                latest = table.current_snapshot()
                if latest is None or latest.snapshot_id != head.snapshot_id:
                    raise RuntimeError('Snapshot сменился во время resume read-back')
                return written_receipt(proof, head.snapshot_id, resumed=True)
        current = table.snapshot_by_id(current.parent_snapshot_id) if current.parent_snapshot_id else None
    return None
