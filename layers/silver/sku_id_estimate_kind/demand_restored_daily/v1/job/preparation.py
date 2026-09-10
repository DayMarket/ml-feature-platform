"""Проверить паспорт E3 и перенести исходные значения без вычисления спроса."""

from datetime import date, datetime, timezone
import json
import math
import re

import pyarrow as pa

MEASURES = ("sales_units","lost_units","demand_units","potential_units","sales_gmv","lost_gmv","demand_gmv","potential_gmv","lost_unit_price","p_active","sigma",)
LINEAGE = ("source_state_version", "source_model_version", "source_code_version", "source_catalog_version")


def target_ref(config, catalog_name):
    table = config["table"]
    for key in ("catalog", "schema", "name"):
        if not isinstance(table.get(key), str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table[key]):
            raise ValueError(f"Неверная компонента table.{key}")
    if table["catalog"] != catalog_name:
        raise ValueError("Другой Iceberg каталог")
    return table["schema"], table["name"]


def utc_naive(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Нужно timezone-aware время")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def validate_schema(schema):
    expected = {
        "date": (pa.date32(), True),
        "sku_id": (pa.int64(), True),
        "estimate_kind": (pa.string(), True),
        "run_id": (pa.string(), True),
        "prediction_date": (pa.date32(), True),
        "sales_units": (pa.float64(), True),
        "lost_units": (pa.float64(), False),
        "demand_units": (pa.float64(), False),
        "potential_units": (pa.float64(), False),
        "sales_gmv": (pa.float64(), False),
        "lost_gmv": (pa.float64(), False),
        "demand_gmv": (pa.float64(), False),
        "potential_gmv": (pa.float64(), False),
        "lost_unit_price": (pa.float64(), False),
        "p_active": (pa.float64(), False),
        "sigma": (pa.float64(), False),
        "currency_code": (pa.string(), True),
        "price_model_version": (pa.string(), True),
        "rate_ok": (pa.int32(), True),
        "settled_at": (pa.date32(), False),
        "method_version": (pa.string(), True),
        "quality_status": (pa.string(), True),
        "unavailable_reason": (pa.string(), True),
        "source_updated_at": (pa.timestamp('us'), True),
        "source_state_version": (pa.int64(), True),
        "source_model_version": (pa.string(), True),
        "source_code_version": (pa.string(), True),
        "source_catalog_version": (pa.string(), True),
        "source_manifest_id": (pa.string(), True),
        "source_contract_version": (pa.string(), True),
        "ingested_at": (pa.timestamp('us'), True),
    }
    if len(schema.names) != len(expected) or set(schema.names) != set(expected):
        raise ValueError("Схема E3 не совпадает с миграцией")
    for name, (kind, required) in expected.items():
        field = schema.field(name)
        if field.type != kind and not (pa.types.is_string(kind) and pa.types.is_large_string(field.type)):
            raise ValueError(f"Неверный тип E3 {name}")
        if required and field.nullable:
            raise ValueError(f"E3 {name} должен быть required")



def validate_run(run, selected):
    from .query import selection
    selection(**selected)
    if not isinstance(run, dict):
        raise ValueError("Нет паспорта E3")
    if run.get("run_id") != selected["run_id"] or run.get("prediction_date") != selected["prediction_date"]:
        raise ValueError("Получен паспорт другого E3-run")
    if run.get("stage") != "e3" or run.get("status") not in {"validated", "published"}:
        raise ValueError("E3 ещё не проверен либо run принадлежит другому этапу")
    if type(run.get("state_version")) is not int or not 0 < run["state_version"] < 2**63:
        raise ValueError("Неверная версия состояния E3")
    for name in ("model_version", "code_version", "catalog_version", "input_manifest", "output_manifest"):
        if not isinstance(run.get(name), str) or not run[name].strip():
            raise ValueError(f"Нет обязательного {name} E3-run")
    for name in ("input_manifest", "output_manifest"):
        manifest = json.loads(run[name])
        if not isinstance(manifest, dict) or not manifest:
            raise ValueError(f"Нет структуры {name}")
    for name in (("finished_at", "published_at") if run["status"] == "published" else ("finished_at",)):
        if not isinstance(run.get(name), datetime) or run[name].utcoffset() is None:
            raise ValueError(f"Нет timezone-aware {name}")
    return {f"source_{name}": run[name] for name in
            ("state_version", "model_version", "code_version", "catalog_version")}


def prepare_batch(raw, schema, *, selected, run, manifest, version, ingested_at):
    validate_schema(schema)
    lineage = validate_run(run, selected)
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("Нужно время захвата с зоной")
    if run["finished_at"] > ingested_at:
        raise ValueError("Завершение E3 позже захвата")
    if any(not isinstance(v, str) or not v.strip() for v in (manifest, version)):
        raise ValueError("Нужны manifest/version переноса")
    technical = {*LINEAGE, "source_manifest_id", "source_contract_version", "ingested_at"}
    if not isinstance(raw, pa.Table) or len(raw.column_names) != len(set(raw.column_names)):
        raise ValueError("Неверный Arrow source")
    if set(raw.column_names) != set(schema.names) - technical:
        raise ValueError("Колонки E3 не совпадают с целевой схемой")
    values = {}
    for field in schema:
        name = field.name
        if name in technical:
            continue
        column = raw[name]
        if name == "source_updated_at":
            if not pa.types.is_timestamp(column.type) or column.type.tz != "UTC":
                raise ValueError("source_updated_at должен быть явно UTC")
        elif pa.types.is_integer(field.type) and not pa.types.is_integer(column.type):
            raise ValueError(f"Нельзя округлять {name} в integer")
        elif name in MEASURES and column.type != pa.float64():
            raise ValueError(f"Нужен исходный Float64 {name}, не строка или округлённый float")
        elif pa.types.is_date(field.type) and not pa.types.is_date(column.type):
            raise ValueError(f"Нужна исходная DATE {name}")
        elif (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)) and not (
                pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
            raise ValueError(f"Нужна исходная строка {name}, без приведения чисел")
        values[name] = column.cast(field.type, safe=True)
    metadata = lineage | {"source_manifest_id": manifest, "source_contract_version": version,
                          "ingested_at": ingested_at.astimezone(timezone.utc).replace(tzinfo=None)}
    for name, value in metadata.items():
        values[name] = pa.array([value] * raw.num_rows, type=schema.field(name).type)
    result = pa.Table.from_arrays([values[f.name] for f in schema], schema=schema)
    return validate_batch(result, schema, selected=selected, run=run, manifest=manifest,
                          version=version, ingested_at=ingested_at)


def validate_batch(result, schema, *, selected, run, manifest, version, ingested_at):
    validate_schema(schema)
    lineage = validate_run(run, selected)
    captured = utc_naive(ingested_at)
    if not isinstance(result, pa.Table) or not result.schema.equals(schema, check_metadata=False):
        raise ValueError("Неверная схема готовой порции E3")
    if any(not isinstance(v, str) or not v.strip() for v in (manifest, version)):
        raise ValueError("Нужны manifest/version переноса")
    if run["finished_at"] > ingested_at or (
            run["status"] == "published" and run["published_at"] > ingested_at):
        raise ValueError("Завершение/публикация E3 позже захвата")
    for field in schema:
        if not field.nullable and result[field.name].null_count:
            raise ValueError(f"NULL в обязательном поле {field.name}")
    previous = None
    for row in result.to_pylist():
        expected = lineage | {"source_manifest_id": manifest, "source_contract_version": version,
                              "ingested_at": captured}
        if any(row[name] != value for name, value in expected.items()):
            raise ValueError("Смешанные версии/capture E3")
        key = row["date"], row["sku_id"], row["estimate_kind"]
        if row["run_id"] != selected["run_id"] or row["prediction_date"] != selected["prediction_date"]:
            raise ValueError("Поток содержит другой E3-run")
        if (type(row["date"]) is not date or not selected["start"] <= row["date"] < selected["end"]
                or row["sku_id"] <= 0 or (previous is not None and key <= previous)):
            raise ValueError("Неверный день/ключ либо дубликат E3")
        previous = key
        if row["estimate_kind"] not in {"provisional", "final"} or row["quality_status"] not in {"ok", "unavailable"}:
            raise ValueError("Неизвестная версия оценки или статус качества")
        if row["rate_ok"] not in (0, 1):
            raise ValueError("rate_ok должен быть 0/1")
        if any(row[n] is not None and not math.isfinite(row[n]) for n in MEASURES):
            raise ValueError("Нечисловая/бесконечная мера E3")
        if row["quality_status"] == "unavailable" and not row["unavailable_reason"].strip():
            raise ValueError("Нет причины недоступности E3")
    return result
