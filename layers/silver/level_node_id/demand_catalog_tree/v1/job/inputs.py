"""Закрепить прошедший DQ SKU capture и схему его точного Iceberg snapshot."""

from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
import re
from uuid import UUID
from zoneinfo import ZoneInfo

import pyarrow as pa
import yaml

from .preparation import INPUT_COLUMNS, target_ref

READ_COLUMNS = (*INPUT_COLUMNS, "source_manifest_id", "source_contract_version", "ingested_at")


def migration_schema(entity_path):
    """Разобрать простую DDL владельца, отказать при неизвестном типе или синтаксисе."""
    ddl = (Path(entity_path) / "migrations/create_table.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS \{target_table\}\s*\((.*?)\n\)\s*USING iceberg", ddl, re.S)
    if body is None:
        raise ValueError("Неподдержанная форма миграции")
    types = {"DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "STRING": pa.string(),
             "INT": pa.int32(), "BIGINT": pa.int64(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_()}
    fields = []
    for line in body[1].splitlines():
        if not line.strip():
            continue
        column = re.fullmatch(r"\s*([a-z_][a-z0-9_]*)\s+([A-Z]+)( NOT NULL)? COMMENT '(?:''|[^'])*',?\s*", line)
        if column is None or column[2] not in types:
            raise ValueError("Неподдержанная колонка миграции")
        fields.append(pa.field(column[1], types[column[2]], nullable=not bool(column[3])))
    if not fields or len({field.name for field in fields}) != len(fields):
        raise ValueError("Пустая/неоднозначная схема миграции")
    return pa.schema(fields)


def validate_schema(actual, expected):
    if len(actual) != len(expected) or set(actual.names) != set(expected.names):
        raise ValueError("Схема таблицы не соответствует миграции владельца")
    for field in expected:
        found = actual.field(field.name)
        same = found.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(found.type)
        if not same or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable {field.name}")


def source_config(config, repo_root):
    root = Path(repo_root).resolve()
    path = (root / config["inputs"]["sku_config"]).resolve()
    if not path.is_relative_to(root):
        raise ValueError("Input config должен находиться внутри FP")
    source = yaml.safe_load(path.read_text(encoding="utf-8"))
    if (source["table"]["key"] != "demand_catalog_sku"
            or source["table"]["primary_key"].replace(" ", "") != "date,sku_id"
            or source["dq"].get("scope") != "partition"
            or source["dq"].get("partition_granularity") != "timestamp"):
        raise ValueError("Неверный владелец/ключ/snapshot-DQ контракт SKU")
    schema = migration_schema(path.parent)
    for name in READ_COLUMNS:
        dtype = (pa.date32() if name == "date" else pa.int64() if name == "sku_id"
                 else pa.timestamp("us") if name == "ingested_at" else pa.string())
        if name not in schema.names or schema.field(name).type != dtype:
            raise ValueError(f"Изменён контракт поля SKU.{name}")
    return source, schema


def validate_reference(source, reference):
    if (not isinstance(reference, dict) or set(reference) != {"dag_id", "run_id"}
            or reference["dag_id"] != source["dag"]["id"]
            or not isinstance(reference["run_id"], str) or not reference["run_id"].strip()):
        raise ValueError("Нужны точные DAG/run владельца SKU")


def bind_source(source, reference, checked, *, captured_at):
    """checked приходит от task=dq точного run; это не поиск latest по дате."""
    if not isinstance(captured_at, datetime) or captured_at.utcoffset() is None:
        raise ValueError("Нужно aware время материализации дерева")
    validate_reference(source, reference)
    if (not isinstance(checked, dict) or checked.get("dq_status") != "passed"
            or any(checked.get(key) != reference[key] for key in reference)):
        raise ValueError("Нет passed DQ точного SKU run")
    receipt = checked.get("receipt")
    if not isinstance(receipt, dict) or receipt.get("status") != "written":
        raise ValueError("Неверный writer receipt SKU")
    if any(type(receipt.get(key)) is not int or not 0 < receipt[key] <= 2**63 - 1
           for key in ("snapshot_id", "rows_written")):
        raise ValueError("Нужны положительные snapshot ID и полный count SKU")
    for key in ("table_uuid", "catalog_version", "source_manifest_id", "source_contract_version", "ingested_at"):
        if not isinstance(receipt.get(key), str) or not receipt[key].strip():
            raise ValueError(f"Нет {key} в SKU receipt")
    try:
        UUID(receipt["table_uuid"])
        moment = datetime.fromisoformat(receipt["ingested_at"].replace("Z", "+00:00"))
        day = date.fromisoformat(receipt["date_min"])
    except (KeyError, ValueError, TypeError) as error:
        raise ValueError("Неверные UUID/date/capture SKU") from error
    if (moment.utcoffset() is None or moment > captured_at or receipt.get("date_max") != day.isoformat()
            or receipt["date_min"] != day.isoformat()
            or moment.astimezone(ZoneInfo("Asia/Tashkent")).date() != day
            or receipt["source_contract_version"] != source["source"]["contract_version"]):
        raise ValueError("SKU capture/дата/контракт не соответствуют выбранному входу")
    return {"reference": deepcopy(reference), "receipt": deepcopy(receipt), "date": day,
            "captured_at": moment.astimezone(timezone.utc)}


def preflight_source(source, catalog, bound, expected_schema):
    identifier = target_ref(source, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier} в {type(catalog).__name__}: сначала миграции")
    table = catalog.load_table(identifier)
    receipt = bound["receipt"]
    snapshot = table.snapshot_by_id(receipt["snapshot_id"])
    if str(table.metadata.table_uuid) != receipt["table_uuid"] or snapshot is None:
        raise ValueError("Точный SKU snapshot/UUID недоступен, latest запрещён")
    schema = table.schemas().get(snapshot.schema_id)
    if schema is None:
        raise ValueError("Недоступна схема точного SKU snapshot")
    arrow = schema.as_arrow()
    validate_schema(arrow, expected_schema)
    return table, arrow
