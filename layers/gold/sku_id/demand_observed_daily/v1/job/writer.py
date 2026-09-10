"""Атомарно заменить gold SKU-день после полного чтения и проверки точных входов."""

from contextlib import ExitStack
from datetime import date
from hashlib import sha256
from itertools import count
import json
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc

from .preparation import metadata, target_ref, validate_batch, validate_schema

PROOF_KEY = "demand-observed-checkpoint-v1"


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


def input_signature(inputs):
    return sha256(json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def preflight_target(config, catalog):
    from pyiceberg.transforms import IdentityTransform

    limits = config["runtime"]
    if any(type(limits[k]) is not int or limits[k] <= 0 for k in ("max_batch_rows", "max_batch_bytes")):
        raise ValueError("Неверные лимиты gold-порций")
    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
    table = catalog.load_table(identifier)
    validate_schema(table.schema().as_arrow())
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field("date").field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError("Gold требует identity partition по date")
    return table


def require_head(config, catalog, table_uuid, snapshot_id):
    table = catalog.load_table(target_ref(config, catalog.name))
    snapshot = table.current_snapshot()
    if (str(table.metadata.table_uuid) != table_uuid
            or (snapshot.snapshot_id if snapshot else None) != snapshot_id):
        raise RuntimeError("Gold snapshot/UUID сменился до завершения записи")


def write_day(config, catalog, batches, *, day, expected_rows, inputs, manifest, version,
              ingested_at, verify_inputs):
    """Проверенный count объединения обязателен; written ещё не означает passed DQ."""
    from pyiceberg.expressions import EqualTo
    from pyiceberg.io.pyarrow import _dataframe_to_data_files
    from .checkpoint import verify_proof, written_receipt

    with ExitStack() as stack:
        batches = iter(batches)
        if callable(getattr(batches, "close", None)):
            stack.callback(batches.close)
        if type(day) is not date or type(expected_rows) is not int or expected_rows <= 0:
            raise ValueError("Нужны DATE и положительный подтверждённый count объединения")
        if not callable(verify_inputs):
            raise ValueError("Нужна проверка точных upstream DQ/snapshots перед commit")
        # Отделить lineage от изменяемого словаря caller до первого чтения потока.
        inputs = json.loads(json.dumps(inputs))
        captured = metadata(inputs, manifest, version, ingested_at)
        signature = input_signature(inputs)
        table = preflight_target(config, catalog)
        schema = table.schema().as_arrow()
        original = table.current_snapshot()
        original_id = original.snapshot_id if original else None
        table_uuid = str(table.metadata.table_uuid)
        descriptors, written, previous = [], 0, None
        properties = {"source_manifest_id": manifest, "source_contract_version": version,
                      "date": day.isoformat()}
        proof = {"date": day.isoformat(), "rows_written": expected_rows, "inputs": inputs,
                 "input_signature": signature, "source_manifest_id": manifest,
                 "source_contract_version": version, "ingested_at": ingested_at.isoformat(),
                 "table_uuid": table_uuid, "batches": descriptors}
        with table.transaction() as transaction:
            transaction.delete(EqualTo("date", day), snapshot_properties=properties)
            data_files, write_uuid, sequence = [], uuid4(), count()
            for batch in batches:
                if (not isinstance(batch, pa.Table) or batch.num_rows > config["runtime"]["max_batch_rows"]
                        or batch.nbytes > config["runtime"]["max_batch_bytes"]):
                    raise ValueError("Превышен размер gold-порции или неверный тип")
                validate_batch(batch, schema, day=day)
                if not batch.num_rows:
                    continue
                for name, value in captured.items():
                    if batch[name].unique().to_pylist() != [value]:
                        raise ValueError(f"Смешанные gold/input metadata: {name}")
                first, last = batch["sku_id"][0].as_py(), batch["sku_id"][-1].as_py()
                if previous is not None and first <= previous:
                    raise ValueError("Дубли/неверный порядок gold SKU между порциями")
                previous, written = last, written + batch.num_rows
                if written > expected_rows:
                    raise ValueError("Строк gold больше подтверждённого count")
                descriptors.append((first, last, batch.num_rows, fingerprint(batch)))
                data_files.extend(_dataframe_to_data_files(table_metadata=transaction.table_metadata,
                                  df=batch, io=table.io, write_uuid=write_uuid, counter=sequence))
            if written != expected_rows:
                raise ValueError("Неполный поток gold observed")
            if verify_inputs() is not True:
                raise ValueError("Точные upstream inputs/DQ не подтверждены перед commit")
            require_head(config, catalog, table_uuid, original_id)
            properties[PROOF_KEY] = json.dumps(proof, separators=(",", ":"))
            with transaction.update_snapshot(snapshot_properties=properties).fast_append() as append:
                for data_file in data_files:
                    append.append_data_file(data_file)
        snapshot = table.current_snapshot()
        if snapshot is None:
            raise RuntimeError("Нет snapshot после gold commit")
        verify_proof(table, proof, snapshot.snapshot_id)
        require_head(config, catalog, table_uuid, snapshot.snapshot_id)
        return written_receipt(proof, snapshot.snapshot_id)
