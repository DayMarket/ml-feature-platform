"""Подготовить разреженное дневное наличие SKU из EOD."""

from datetime import datetime, timezone
import re

import pyarrow as pa
import pyarrow.compute as pc

RAW_COLUMNS = ("date", "sku_id")
REQUIRED = (
    "date",
    "sku_id",
    "source_manifest_id",
    "source_contract_version",
    "ingested_at",
)


def target_ref(config, catalog_name):
    table = config["table"]
    for key in ("catalog", "schema", "name"):
        value = table.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Неверная компонента table.{key}")
    if table["catalog"] != catalog_name:
        raise ValueError("Другой Iceberg каталог")
    return table["schema"], table["name"]


def validate_schema(schema):
    expected = {
        "date": pa.date32(),
        "sku_id": pa.int64(),
        "source_manifest_id": pa.string(),
        "source_contract_version": pa.string(),
        "ingested_at": pa.timestamp("us"),
    }
    if len(schema) != len(expected) or set(schema.names) != set(expected):
        raise ValueError("Не совпадает набор колонок stock migration")
    for name, dtype in expected.items():
        field = schema.field(name)
        same_string = pa.types.is_string(dtype) and pa.types.is_large_string(field.type)
        if field.type != dtype and not same_string:
            raise ValueError(f"Неверный тип {name}: {field.type}")
        if field.nullable:
            raise ValueError(f"{name} должен быть required")


def utc_naive(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Нужен timezone-aware capture")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def prepare_batch(raw, schema, *, day, manifest, version, ingested_at):
    """Добавить только FP-lineage; бизнес-контракт stock — date и sku_id."""
    validate_schema(schema)
    captured = utc_naive(ingested_at)
    if any(not isinstance(value, str) or not value.strip() for value in (manifest, version)):
        raise ValueError("Нужны непустые manifest/version")
    if not isinstance(raw, pa.Table) or tuple(raw.column_names) != RAW_COLUMNS:
        raise ValueError("Неверная схема source Arrow batch")
    if not pa.types.is_date(raw["date"].type) or not pa.types.is_integer(raw["sku_id"].type):
        raise ValueError("Source требует DATE и целый sku_id")
    arrays = {
        "date": raw["date"].cast(pa.date32(), safe=True),
        "sku_id": raw["sku_id"].cast(pa.int64(), safe=True),
        "source_manifest_id": pa.array(
            [manifest] * raw.num_rows, type=schema.field("source_manifest_id").type
        ),
        "source_contract_version": pa.array(
            [version] * raw.num_rows, type=schema.field("source_contract_version").type
        ),
        "ingested_at": pa.array([captured] * raw.num_rows, type=pa.timestamp("us")),
    }
    prepared = pa.Table.from_arrays([arrays[field.name] for field in schema], schema=schema)
    return validate_batch(prepared, schema, day=day)


def validate_batch(batch, schema, *, day):
    validate_schema(schema)
    if not isinstance(batch, pa.Table) or not batch.schema.equals(schema, check_metadata=False):
        raise ValueError("Batch не соответствует схеме Iceberg")
    if batch.num_rows == 0:
        return batch
    if any(batch[name].null_count for name in REQUIRED):
        raise ValueError("NULL в обязательном stock поле")
    dates = batch["date"].unique().to_pylist()
    ids = batch["sku_id"]
    if dates != [day] or pc.min(ids).as_py() <= 0 or pc.max(ids).as_py() > 2**63 - 1:
        raise ValueError("Неверный день или sku_id")
    if batch.num_rows > 1 and pc.any(
        pc.less_equal(ids.slice(1), ids.slice(0, batch.num_rows - 1))
    ).as_py():
        raise ValueError("Повтор или неверный порядок sku_id")
    return batch
