"""Закрепить точный passed seller run и доступность его Iceberg snapshot."""

from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
import re
from uuid import UUID
from zoneinfo import ZoneInfo

import pyarrow as pa
import yaml

from .preparation import SELLER_FIELDS, target_ref


def migration_schema(entity_path):
    ddl = (Path(entity_path) / "migrations/create_table.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS \{target_table\}\s*\((.*?)\n\)\s*USING iceberg", ddl, re.S)
    if body is None:
        raise ValueError("Неподдержанная форма миграции")
    types = {"DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "STRING": pa.string(),
             "BIGINT": pa.int64(), "BOOLEAN": pa.bool_(), "INT": pa.int32(), "DOUBLE": pa.float64()}
    fields = []
    for line in body[1].splitlines():
        if not line.strip():
            continue
        column = re.fullmatch(r"\s*([a-z_][a-z0-9_]*)\s+([A-Z]+)( NOT NULL)? COMMENT '(?:''|[^'])*',?\s*", line)
        if column is None or column[2] not in types:
            raise ValueError("Неподдержанная колонка миграции")
        fields.append(pa.field(column[1], types[column[2]], nullable=not bool(column[3])))
    schema = pa.schema(fields)
    if not fields or len(set(schema.names)) != len(fields):
        raise ValueError("Пустая/неоднозначная схема миграции")
    return schema


def source_config(config, repo_root):
    root = Path(repo_root).resolve()
    path = (root / config["inputs"]["seller_config"]).resolve()
    if not path.is_relative_to(root):
        raise ValueError("Input config должен находиться внутри FP")
    source = yaml.safe_load(path.read_text(encoding="utf-8"))
    if (source["table"]["key"] != "demand_catalog_seller"
            or source["table"]["primary_key"].replace(" ", "") != "date,seller_id"
            or source["dq"].get("scope") != "partition"
            or source["dq"].get("partition_granularity") != "timestamp"):
        raise ValueError("Неверный владелец/ключ/snapshot-DQ контракт seller")
    schema = migration_schema(path.parent)
    expected_types = {"date": pa.date32(), "seller_id": pa.int64(), "has_master": pa.bool_(), "is_1p": pa.bool_(),
                      "seller_registered_at": pa.timestamp("us"), "ingested_at": pa.timestamp("us")}
    required = {"date", "seller_id", "seller_mapping_status", "catalog_version", "source_contract_version",
                "source_manifest_id", "ingested_at"}
    names = {"date", "seller_id", *SELLER_FIELDS, "catalog_version", "source_contract_version", "source_manifest_id", "ingested_at"}
    if len(schema) != len(names) or set(schema.names) != names:
        raise ValueError("Seller миграция должна содержать 12 согласованных полей")
    for field in schema:
        if field.type != expected_types.get(field.name, pa.string()) or field.nullable != (field.name not in required):
            raise ValueError(f"Изменён контракт поля seller.{field.name}")
    return source, schema


def validate_reference(source, reference):
    if (not isinstance(reference, dict) or set(reference) != {"dag_id", "run_id"}
            or reference["dag_id"] != source["dag"]["id"]
            or not isinstance(reference["run_id"], str) or not reference["run_id"].strip()):
        raise ValueError("Нужны точные DAG/run владельца seller")


def bind_source(source, reference, checked, *, captured_at):
    """checked читается из task=dq указанного run, без include_prior_dates/latest."""
    if not isinstance(captured_at, datetime) or captured_at.utcoffset() is None:
        raise ValueError("Нужно aware время материализации SKU")
    validate_reference(source, reference)
    if (not isinstance(checked, dict) or checked.get("dq_status") != "passed"
            or any(checked.get(key) != reference[key] for key in reference)):
        raise ValueError("Нет passed DQ точного seller run")
    receipt = checked.get("receipt")
    if not isinstance(receipt, dict) or receipt.get("status") != "written":
        raise ValueError("Неверный writer receipt seller")
    if any(type(receipt.get(key)) is not int or not 0 < receipt[key] <= 2**63 - 1
           for key in ("snapshot_id", "rows_written")):
        raise ValueError("Нужны положительные snapshot ID и полный count seller")
    for key in ("table_uuid", "catalog_version", "source_manifest_id", "source_contract_version", "ingested_at"):
        if not isinstance(receipt.get(key), str) or not receipt[key].strip():
            raise ValueError(f"Нет {key} в seller receipt")
    try:
        if UUID(receipt["table_uuid"]).int == 0:
            raise ValueError("Нулевой UUID")
        moment = datetime.fromisoformat(receipt["ingested_at"].replace("Z", "+00:00"))
        day = date.fromisoformat(receipt["date_min"])
    except (KeyError, ValueError, TypeError) as error:
        raise ValueError("Неверные UUID/date/capture seller") from error
    if (moment.utcoffset() is None or moment > captured_at or receipt.get("date_max") != day.isoformat()
            or receipt["date_min"] != day.isoformat()
            or moment.astimezone(ZoneInfo("Asia/Tashkent")).date() != day
            or receipt["source_contract_version"] != source["source"]["contract_version"]):
        raise ValueError("Seller capture/дата/контракт не соответствуют выбранному входу")
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
        raise ValueError("Точный seller snapshot/UUID недоступен, latest запрещён")
    schema = table.schemas().get(snapshot.schema_id)
    if schema is None:
        raise ValueError("Недоступна схема точного seller snapshot")
    actual = schema.as_arrow()
    if len(actual) != len(expected_schema) or set(actual.names) != set(expected_schema.names):
        raise ValueError("Схема seller snapshot не соответствует миграции владельца")
    for field in expected_schema:
        found = actual.field(field.name)
        same = found.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(found.type)
        if not same or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable seller.{field.name}")
    return table, actual
