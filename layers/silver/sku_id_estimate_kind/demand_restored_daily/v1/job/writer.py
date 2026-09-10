"""Атомарно заменить один день E3 ограниченными Arrow-порциями и сверить запись."""

from hashlib import sha256
from itertools import count
import json
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc

from .preparation import target_ref, utc_naive, validate_batch, validate_schema
from .checkpoint import PROOF_KEY, verify_proof, written_receipt
from .manifest import ContentDigest, day_manifest


def fingerprint(batch):
    """Хешировать значения и NULL-маски независимо от chunks и скрытых null-буферов."""
    schema = pa.schema([pa.field(f.name, f.type, nullable=f.nullable) for f in batch.schema])
    arrays = []
    indices = pc.indices_nonzero(pa.repeat(True, batch.num_rows))
    for field in schema:
        column = pc.take(batch[field.name], indices).combine_chunks()
        default = "" if pa.types.is_string(field.type) or pa.types.is_large_string(field.type) else 0
        if pa.types.is_boolean(field.type):
            default = False
        arrays.extend([pc.is_valid(column), pc.fill_null(column, pa.scalar(default, type=field.type))])
    clean = pa.Table.from_arrays(arrays, names=[f"c{index}" for index in range(len(arrays))])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, clean.schema) as stream:
        stream.write_table(clean)
    digest = sha256(schema.serialize())
    digest.update(sink.getvalue())
    return digest.hexdigest()


def write_day(config, catalog, batches, *, day, selected, run, manifest, version,
              ingested_at, verify_source):
    """Receipt written не равен passed DQ. Незавершённый день перезаписывается целиком."""
    from pyiceberg.expressions import EqualTo
    from pyiceberg.io.pyarrow import _dataframe_to_data_files
    from pyiceberg.transforms import IdentityTransform

    expected = day_manifest(config, selected, run, day)
    expected_rows = expected['rows']
    if not callable(verify_source):
        raise ValueError('Нужна повторная source run/hold проверка перед commit')
    if any(not isinstance(v, str) or not v.strip() for v in (manifest, version)):
        raise ValueError('Пустой manifest/version')
    captured = utc_naive(ingested_at)
    limits = config['runtime']
    max_rows, max_bytes = limits['max_batch_rows'], limits['max_batch_bytes']
    if any(type(v) is not int or v <= 0 for v in (max_rows, max_bytes)):
        raise ValueError('Неверные лимиты порций')
    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f'Нет таблицы {identifier}: сначала применить миграцию')
    table = catalog.load_table(identifier)
    schema = table.schema().as_arrow()
    validate_schema(schema)
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field('date').field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError('Нужен identity partition по date')
    scope = EqualTo('date', day)
    descriptors, previous, written = [], None, 0
    content = ContentDigest()
    properties = {'source_manifest_id': manifest, 'source_contract_version': version, 'date': day.isoformat()}
    proof = {'date': day.isoformat(), 'rows_written': expected_rows,
             'source_manifest_id': manifest, 'source_contract_version': version,
             'ingested_at': ingested_at.isoformat(), 'table_uuid': str(table.metadata.table_uuid),
             'batches': descriptors, 'source_day': expected}
    with table.transaction() as transaction:
        transaction.delete(scope, snapshot_properties=properties)
        # Data files ещё не видны читателям; proof входит в тот же атомарный commit.
        data_files, write_uuid = [], uuid4()
        sequence = count()
        for batch in batches:
            if not isinstance(batch, pa.Table) or batch.num_rows > max_rows or batch.nbytes > max_bytes:
                raise ValueError('Превышен размер порции или неверный тип')
            validate_batch(batch, schema, selected=selected, run=run, manifest=manifest,
                           version=version, ingested_at=ingested_at)
            if batch.num_rows and batch['date'].unique().to_pylist() != [day]:
                raise ValueError('Порция другого дня E3')
            if not batch.num_rows:
                continue
            for name, value in [('source_manifest_id', manifest), ('source_contract_version', version),
                                   ('ingested_at', captured)]:
                if batch[name].unique().to_pylist() != [value]:
                    raise ValueError(f'Смешанные {name}')
            content.update(batch)
            first = (batch['sku_id'][0].as_py(), batch['estimate_kind'][0].as_py())
            last = (batch['sku_id'][-1].as_py(), batch['estimate_kind'][-1].as_py())
            if previous is not None and first <= previous:
                raise ValueError('Повтор или неверный порядок SKU между порциями')
            previous = last
            written += batch.num_rows
            if written > expected_rows:
                raise ValueError('Строк больше source count')
            descriptors.append((first, last, batch.num_rows, fingerprint(batch)))
            for data_file in _dataframe_to_data_files(table_metadata=transaction.table_metadata, df=batch,
                                                      io=table.io, write_uuid=write_uuid, counter=sequence):
                data_files.append(data_file)
        if written != expected_rows:
            raise ValueError('Неполный поток E3')
        content.verify(expected)
        if verify_source() is not True:
            raise ValueError('Source run/hold изменился или не подтверждён перед commit')
        properties[PROOF_KEY] = json.dumps(proof, separators=(',', ':'))
        with transaction.update_snapshot(snapshot_properties=properties).fast_append() as append:
            for data_file in data_files:
                append.append_data_file(data_file)
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError('Нет snapshot после E3 commit')
    verify_proof(table, proof, snapshot.snapshot_id)
    table.refresh()
    current = table.current_snapshot()
    if current is None or current.snapshot_id != snapshot.snapshot_id:
        raise RuntimeError('Snapshot сменился во время read-back')
    return written_receipt(proof, snapshot.snapshot_id)
