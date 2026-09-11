"""Прочитать точный snapshot календаря и полный реестр акций после preflight."""

from datetime import datetime, timezone
from pathlib import Path
import re

import pyarrow as pa
import yaml

from .preparation import PROMO_FIELDS, PROMO_TEXT, PROMO_TIMES, prepare_events, validate_target_schema


def target_ref(config, catalog_name):
    """Адресовать существующую таблицу через два компонента, без SQL-prefix parsing."""
    table = config["table"]
    for name in ("catalog", "schema", "name"):
        value = table[name]
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"table.{name}: нужен отдельный компонент identifier")
    if table["catalog"] != catalog_name:
        raise ValueError("Загружен другой Iceberg catalog")
    return table["schema"], table["name"]


def source_sql(config):
    source = config["source"]
    if (source["engine"], source["schema"], source["name"]) != ("clickhouse", "silver", "b2b_marketing_sale"):
        raise ValueError("Источник не совпадает с согласованным silver.b2b_marketing_sale")
    columns = ["id", *PROMO_TEXT, *(f"toTimeZone({name}, 'UTC') AS {name}" for name in PROMO_TIMES)]
    return "SELECT " + ", ".join(columns) + " FROM silver.b2b_marketing_sale ORDER BY id"


def source_from_records(rows, column_types):
    """Сохранить нативные строки/целые/UTC по метаданным SELECT, без Pandas."""
    names = [name for name, _ in column_types]
    if len(names) != len(PROMO_FIELDS) or set(names) != set(PROMO_FIELDS):
        raise ValueError("Метаданные реестра не совпадают с 9 полями SELECT")
    if any(len(row) != len(names) for row in rows):
        raise ValueError("Строка не совпадает с метаданными ClickHouse")
    arrays = []
    for index, (name, declared_type) in enumerate(column_types):
        nullable, kind = False, declared_type
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            wrapper, kind = kind.split("(", 1)
            nullable = nullable or wrapper == "Nullable"
            kind = kind[:-1]
        values = [row[index] for row in rows]
        if not nullable and any(value is None for value in values):
            raise ValueError(f"NULL противоречит исходному типу {name}: {declared_type}")
        if name == "id" and re.fullmatch(r"U?Int(8|16|32|64)", kind):
            dtype = pa.uint64() if kind.startswith("U") else pa.int64()
            allowed = int
        elif name in PROMO_TEXT and kind == "String":
            dtype, allowed = pa.string(), str
        elif name in PROMO_TIMES and (kind == "DateTime('UTC')"
                                     or re.fullmatch(r"DateTime64\([0-6],\s*'UTC'\)", kind)):
            dtype, allowed = pa.timestamp("us", "UTC"), datetime
        else:
            raise ValueError(f"Несовместимый исходный тип {name}: {declared_type}")
        if any(value is not None and type(value) is not allowed for value in values):
            raise ValueError(f"Значение {name} не соответствует исходному типу {declared_type}")
        if name in PROMO_TIMES:
            # Драйвер может вернуть naive datetime: зона известна из типа SELECT, не из сервера.
            values = [None if value is None else value.replace(tzinfo=timezone.utc)
                      if value.utcoffset() is None else value.astimezone(timezone.utc) for value in values]
        arrays.append(pa.array(values, type=dtype, safe=True))
    return pa.Table.from_arrays(arrays, names=names).select(PROMO_FIELDS)


def _existing(config, catalog):
    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет Iceberg таблицы {identifier}: проверить каталог и применение миграции")
    return catalog.load_table(identifier)


def _calendar_config(config, repo_root):
    root = Path(repo_root).resolve()
    path = (root / config["inputs"]["calendar_config"]).resolve()
    if not path.is_relative_to(root):
        raise ValueError("calendar_config должен находиться внутри FP-репозитория")
    result = yaml.safe_load(path.read_text(encoding="utf-8"))
    if result["table"]["key"] != "demand_calendar" or result["source"]["calendar_id"] != "uz_official":
        raise ValueError("Нужен config владельца официального календаря")
    return result


def read_calendar(table, receipt):
    """Прочитать receipt запуска, чей dq проверит оркестратор; не выбирать latest."""
    if receipt.get("status") != "written" or receipt.get("calendar_id") != "uz_official":
        raise ValueError("Нужен receipt полного календарного захвата")
    snapshot_id = receipt.get("snapshot_id")
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise ValueError("Нужен точный snapshot_id календаря")
    if str(table.metadata.table_uuid) != receipt.get("table_uuid"):
        raise ValueError("Receipt относится к другой Iceberg таблице календаря")
    if table.snapshot_by_id(snapshot_id) is None:
        raise ValueError("Snapshot календаря недоступен; latest не заменяет проверенную версию")
    batch = table.scan(snapshot_id=snapshot_id).to_arrow()
    if batch.num_rows == 0 or batch.num_rows != receipt.get("rows_written"):
        raise ValueError("Число строк календаря не совпадает с receipt")
    required = {"date", "calendar_id", "source_manifest_id", "ingested_at", "is_public_holiday", "holiday_name"}
    if not required.issubset(batch.column_names):
        raise ValueError("Snapshot не содержит обязательных полей календаря")
    if set(batch["calendar_id"].to_pylist()) != {"uz_official"}:
        raise ValueError("Snapshot содержит другой источник календаря")
    source_id = receipt.get("source_manifest_id")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("Нет source_manifest_id календаря")
    if set(batch["source_manifest_id"].to_pylist()) != {source_id}:
        raise ValueError("Manifest календаря не совпадает с receipt")
    try:
        captured = datetime.fromisoformat(receipt["ingested_at"])
        if captured.utcoffset() is None:
            raise ValueError("naive capture")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Неверное UTC время receipt календаря") from exc
    for value in batch["ingested_at"].to_pylist():
        if value is None or not isinstance(value, datetime):
            raise ValueError("Нет времени захвата в календаре")
        actual = value.replace(tzinfo=timezone.utc) if value.utcoffset() is None else value
        if actual != captured:
            raise ValueError("Время захвата календаря не совпадает с receipt")
    if batch["date"].type != pa.date32() or batch["date"].null_count:
        raise ValueError("calendar.date должен быть DATE без NULL")
    days = batch["date"].to_pylist()
    if len(set(days)) != len(days):
        raise ValueError("Повтор ключа date в календаре")
    if (min(days).isoformat(), max(days).isoformat()) != (receipt.get("date_min"), receipt.get("date_max")):
        raise ValueError("Диапазон дат календаря не совпадает с receipt")
    return batch


def extract_prepared(config, catalog, repo_root, *, calendar_receipt,
                     source_manifest_id, ingested_at, query_records=None):
    """Preflight обоих FP-объектов, точный календарь, полный SELECT акций; без записи."""
    if not isinstance(source_manifest_id, str) or not source_manifest_id.strip():
        raise ValueError("Нужен source_manifest_id нового захвата")
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("ingested_at должен содержать часовую зону")
    sql = source_sql(config)
    connection = config["source"].get("clickhouse_conn_id")
    if not isinstance(connection, str) or not connection.strip():
        raise ValueError("Не указан подтверждённый ClickHouse connection")
    target = _existing(config, catalog)
    schema = target.schema().as_arrow()
    validate_target_schema(schema)
    calendar_config = _calendar_config(config, repo_root)
    calendar_table = _existing(calendar_config, catalog)
    calendar = read_calendar(calendar_table, calendar_receipt)
    if query_records is None:
        from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

        with ClickHouseHook(clickhouse_conn_id=connection, use_numpy=False).get_conn() as client:
            rows, column_types = client.execute(sql, with_column_types=True)
    else:
        rows, column_types = query_records(sql)
    promos = source_from_records(rows, column_types)
    if promos.num_rows == 0:
        raise ValueError("Пустой реестр акций: запись заблокирована, прежние события сохраняются")
    batch, report = prepare_events(calendar, promos, schema,
                                   source_manifest_id=source_manifest_id, ingested_at=ingested_at)
    report["calendar_source"] = {
        "catalog": calendar_config["table"]["catalog"],
        "identifier": list(target_ref(calendar_config, catalog.name)),
        "receipt": dict(calendar_receipt),
    }
    report["promo_source"] = {"engine": "clickhouse", "schema": config["source"]["schema"],
                              "name": config["source"]["name"], "query": sql,
                              "column_types": [list(pair) for pair in column_types]}
    return batch, report
