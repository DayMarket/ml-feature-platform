"""Закрепить точный passed seller-sales run, полный день и схему выбранного snapshot."""

from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
import re

import pyarrow as pa
import yaml

from dq.day_range import capture_time, validate_written
from .preparation import target_ref, validate_schema
from .seller_rollup import validate_seller_schema


def migration_schema(entity):
    ddl = (Path(entity) / "migrations/create_table.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS \{target_table\}\s*\((.*?)\n\)\s*USING iceberg", ddl, re.S)
    if body is None:
        raise ValueError("Неподдержанная форма миграции")
    types = {"DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "STRING": pa.string(),
             "BIGINT": pa.int64(), "DOUBLE": pa.float64(), "DECIMAL(38,0)": pa.decimal128(38, 0)}
    fields = []
    for line in body[1].splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r"\s*([a-z_][a-z0-9_]*)\s+([A-Z]+(?:\(\d+,\d+\))?)( NOT NULL)? COMMENT '(?:''|[^'])*',?\s*", line)
        if match is None or match[2] not in types:
            raise ValueError("Неподдержанная колонка миграции")
        fields.append(pa.field(match[1], types[match[2]], nullable=not bool(match[3])))
    if not fields or len({f.name for f in fields}) != len(fields):
        raise ValueError("Пустая/неоднозначная схема миграции")
    return pa.schema(fields)


def source_config(config, repo_root):
    root = Path(repo_root).resolve()
    path = (root / config["inputs"]["seller_config"]).resolve()
    if not path.is_relative_to(root):
        raise ValueError("Input config должен находиться внутри FP")
    source = yaml.safe_load(path.read_text(encoding="utf-8"))
    if (source["table"]["key"] != "demand_seller_sales_observed_daily"
            or source["table"]["primary_key"].replace(" ", "") != "date,sku_id,seller_key"):
        raise ValueError("Неверный владелец/ключ seller-sales")
    expected = migration_schema(path.parent)
    target = migration_schema(Path(__file__).resolve().parents[1])
    validate_seller_schema(expected, target)
    return source, expected


def bind_source(source, reference, checked, *, days, captured_at):
    if (not isinstance(reference, dict) or set(reference) != {"dag_id", "run_id"}
            or reference["dag_id"] != source["dag"]["id"]
            or not isinstance(reference["run_id"], str) or not reference["run_id"].strip()):
        raise ValueError("Нужны точные dag_id/run_id владельца seller-sales")
    if (not isinstance(days, list) or not days or any(type(day) is not date for day in days)
            or days != sorted(set(days)) or not isinstance(captured_at, datetime) or captured_at.utcoffset() is None):
        raise ValueError("Нужны даты SKU-sales и aware capture")
    if (not isinstance(checked, dict) or checked.get("dq_status") != "passed"
            or any(checked.get(k) != v for k, v in reference.items())):
        raise ValueError("Нет passed DQ точного seller-sales run")
    written = checked.get("receipt")
    covered = validate_written(written)
    if written["snapshot_id"] > 2**63 - 1:
        raise ValueError("Snapshot ID не помещается в BIGINT")
    checks = checked.get("day_checks")
    if not isinstance(checks, list) or len(checks) != len(covered) or not set(days).issubset(covered):
        raise ValueError("Нет полного DQ запрошенных seller-sales дней")
    selected = {}
    for day, receipt, check in zip(covered, written["day_receipts"], checks, strict=True):
        expected = {"date": day.isoformat(), "dq_status": "passed", **reference,
                    "request_id": written["request_id"], "snapshot_id": written["snapshot_id"],
                    "table_uuid": written["table_uuid"], "source_manifest_id": receipt["source_manifest_id"],
                    "rows_checked": receipt["rows_written"]}
        if (not isinstance(check, dict) or any(check.get(k) != v or type(check.get(k)) is not type(v)
                                              for k, v in expected.items())):
            raise ValueError("Дневной DQ не соответствует seller-sales receipt")
        if (receipt["source_contract_version"] != source["source"]["contract_version"]
                or capture_time(receipt) > captured_at):
            raise ValueError("Неверный seller-sales source contract/capture")
        if day in days:
            selected[day.isoformat()] = deepcopy(receipt)
    return {**deepcopy(reference), "request_id": written["request_id"], "snapshot_id": written["snapshot_id"],
            "table_uuid": written["table_uuid"], "day_receipts": selected}


def day_input(bound, day):
    if type(day) is not date or day.isoformat() not in bound["day_receipts"]:
        raise ValueError("Нет выбранного seller-sales дня")
    return deepcopy({k: v for k, v in bound.items() if k != "day_receipts"}) | {
        "day_receipt": deepcopy(bound["day_receipts"][day.isoformat()])}


def preflight_source(source, catalog, bound, expected_schema):
    identifier = target_ref(source, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
    table = catalog.load_table(identifier)
    snapshot = table.snapshot_by_id(bound["snapshot_id"])
    if str(table.metadata.table_uuid) != bound["table_uuid"] or snapshot is None:
        raise ValueError("Точный seller-sales snapshot/UUID недоступен, latest запрещён")
    schema = table.schemas().get(snapshot.schema_id)
    if schema is None:
        raise ValueError("Недоступна схема выбранного seller-sales snapshot")
    actual = schema.as_arrow()
    if len(actual) != len(expected_schema) or set(actual.names) != set(expected_schema.names):
        raise ValueError("Seller-sales snapshot не соответствует DDL владельца")
    for field in expected_schema:
        found = actual.field(field.name)
        same = found.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(found.type)
        if not same or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable seller-sales.{field.name}")
    return table, actual


def preflight_target(config, catalog):
    from pyiceberg.transforms import IdentityTransform

    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
    table = catalog.load_table(identifier)
    validate_schema(table.schema().as_arrow())
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field("date").field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError("SKU-sales требует identity partition по date")
    return table
