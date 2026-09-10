"""Подготовить полный observed-календарь без записи или публикации Iceberg release."""

from __future__ import annotations

from datetime import date, datetime, timezone
import re

import pyarrow as pa
import pyarrow.compute as pc


SOURCE_FIELDS = (
    "dt", "year", "quarter", "month", "month_name_en", "month_abbr_en", "day",
    "day_of_week_iso", "day_name_en", "day_abbr_en", "iso_week", "is_weekend",
    "is_public_holiday", "holiday_name", "is_working_day",
)
FLAGS = {"is_weekend", "is_public_holiday", "is_working_day"}
NUMBERS = {"year", "quarter", "month", "day", "day_of_week_iso", "iso_week"}
TEXT = {"month_name_en", "month_abbr_en", "day_name_en", "day_abbr_en", "holiday_name"}


def _component(value, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{field}: нужен отдельный непустой компонент identifier")
    return value


def target_ref(config, catalog_name: str) -> tuple[str, str]:
    table = config["table"]
    configured = _component(table["catalog"], "table.catalog")
    if configured != catalog_name:
        raise ValueError("Загружен другой Iceberg catalog")
    return (_component(table["schema"], "table.schema"),
            _component(table["name"], "table.name"))


def validate_target_schema(schema: pa.Schema) -> None:
    names = ("date", "calendar_id", *SOURCE_FIELDS[1:], "source_manifest_id", "ingested_at")
    if tuple(schema.names) != names:
        raise ValueError("Схема calendar не совпадает с контрактом 18 полей")
    required = {"date", "calendar_id", "source_manifest_id", "ingested_at"}
    for field in schema:
        dtype = field.type
        if field.name == "date":
            valid = dtype == pa.date32()
        elif field.name in NUMBERS:
            valid = dtype == pa.int32()
        elif field.name in FLAGS:
            valid = dtype == pa.bool_()
        elif field.name == "ingested_at":
            valid = pa.types.is_timestamp(dtype) and dtype.unit == "us" and dtype.tz in (None, "UTC")
        else:
            valid = pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
        if not valid or field.nullable != (field.name not in required):
            raise ValueError(f"Несовместимое поле Iceberg calendar: {field.name}")


def _provenance(config, source_manifest_id: str, ingested_at: datetime) -> None:
    if config["source"]["calendar_id"] != "uz_official":
        raise ValueError("Поддерживается только согласованный calendar_id=uz_official")
    if not isinstance(source_manifest_id, str) or not source_manifest_id.strip():
        raise ValueError("Нужен source_manifest_id захваченного источника")
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("ingested_at должен содержать часовую зону")


def source_sql(config) -> str:
    source = config["source"]
    if source["engine"] != "clickhouse" or (source["schema"], source["name"]) != ("silver", "calendar"):
        raise ValueError("Источник не совпадает с согласованным silver.calendar")
    return "SELECT " + ", ".join(SOURCE_FIELDS) + " FROM silver.calendar ORDER BY dt"


def prepare_calendar(source: pa.Table, schema: pa.Schema, config, *,
                     source_manifest_id: str, ingested_at: datetime) -> pa.Table:
    """Сохранить исходные значения, отвергнуть неоднозначные типы и дубли дат."""
    validate_target_schema(schema)
    _provenance(config, source_manifest_id, ingested_at)
    if len(source.column_names) != len(SOURCE_FIELDS) or set(source.column_names) != set(SOURCE_FIELDS):
        raise ValueError("Источник должен содержать ровно 15 объявленных полей")
    if source.num_rows == 0:
        raise ValueError("Пустой календарь не является готовым полным захватом")
    days = source["dt"]
    if days.type != pa.date32() or days.null_count:
        raise ValueError("dt должен быть DATE без NULL, не timestamp или строка")
    if pc.count_distinct(days).as_py() != source.num_rows:
        raise ValueError("Повтор даты в источнике calendar")
    columns = {}
    for name in SOURCE_FIELDS:
        values = source[name]
        target = "date" if name == "dt" else name
        dtype = values.type
        if name in FLAGS:
            if not (pa.types.is_boolean(dtype) or pa.types.is_integer(dtype) or pa.types.is_null(dtype)):
                raise ValueError(f"{name}: флаг должен быть BOOLEAN или целым 0/1/NULL")
            if pa.types.is_integer(dtype):
                invalid = pc.and_(pc.is_valid(values),
                                  pc.invert(pc.is_in(values, value_set=pa.array([0, 1], type=dtype))))
                if pc.any(invalid).as_py():
                    raise ValueError(f"{name}: неизвестное значение флага")
        elif name in NUMBERS:
            if not (pa.types.is_integer(dtype) or pa.types.is_null(dtype)):
                raise ValueError(f"{name}: требуется целое число или NULL")
        elif name in TEXT:
            if not (pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_null(dtype)):
                raise ValueError(f"{name}: требуется строка или NULL")
        columns[target] = pc.cast(values, schema.field(target).type, safe=True)
    moment = ingested_at.astimezone(timezone.utc)
    if schema.field("ingested_at").type.tz is None:
        moment = moment.replace(tzinfo=None)
    for name, value in (("calendar_id", config["source"]["calendar_id"]),
                        ("source_manifest_id", source_manifest_id), ("ingested_at", moment)):
        columns[name] = pa.array([value] * source.num_rows, type=schema.field(name).type)
    result = pa.Table.from_arrays([columns[name] for name in schema.names], schema=schema)
    result.validate(full=True)
    return result.sort_by([("date", "ascending")])


def extract_prepared(config, catalog, *, source_manifest_id: str,
                     ingested_at: datetime, query_dataframe=None) -> pa.Table:
    """Preflight до чтения ClickHouse, затем подготовка Arrow; write API не вызываются."""
    identifier = target_ref(config, catalog.name)
    _provenance(config, source_manifest_id, ingested_at)
    sql = source_sql(config)
    conn_id = config["source"].get("clickhouse_conn_id")
    if not isinstance(conn_id, str) or not conn_id.strip():
        raise ValueError("Не указан подтверждённый ClickHouse connection")
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет Iceberg таблицы {identifier}: проверить каталог и применение миграции")
    table = catalog.load_table(identifier)
    schema = table.schema().as_arrow()
    validate_target_schema(schema)
    if query_dataframe is None:
        from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

        # NumPy/Pandas превращает ClickHouse DATE в timestamp и теряет исходный тип.
        with ClickHouseHook(clickhouse_conn_id=conn_id, use_numpy=False).get_conn() as client:
            rows, column_types = client.execute(sql, with_column_types=True)
        source = source_from_records(rows, column_types)
    else:
        source = pa.Table.from_pandas(query_dataframe(sql), preserve_index=False)
    return prepare_calendar(source, schema, config,
                            source_manifest_id=source_manifest_id, ingested_at=ingested_at)


def source_from_records(rows, column_types) -> pa.Table:
    """Сохранить DATE/целые/NULL по типам драйвера, без промежуточного Pandas."""
    names = [name for name, _ in column_types]
    if len(names) != len(SOURCE_FIELDS) or set(names) != set(SOURCE_FIELDS):
        raise ValueError("Метаданные ClickHouse должны содержать ровно 15 полей календаря")
    if any(len(row) != len(names) for row in rows):
        raise ValueError("Число значений строки не совпадает с метаданными ClickHouse")
    arrays = []
    for index, (name, declared_type) in enumerate(column_types):
        nullable = False
        kind = declared_type
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            wrapper, kind = kind.split("(", 1)
            nullable = nullable or wrapper == "Nullable"
            kind = kind[:-1]
        values = [row[index] for row in rows]
        if not nullable and any(value is None for value in values):
            raise ValueError(f"NULL противоречит исходному типу {name}: {declared_type}")
        if name == "dt" and kind in ("Date", "Date32"):
            dtype, allowed = pa.date32(), (date,)
        elif name in TEXT and kind == "String":
            dtype, allowed = pa.string(), (str,)
        elif name in FLAGS and kind == "Bool":
            dtype, allowed = pa.bool_(), (bool,)
        elif name in NUMBERS | FLAGS and re.fullmatch(r"U?Int(8|16|32|64)", kind):
            dtype = pa.uint64() if kind.startswith("U") else pa.int64()
            allowed = (int,)
        else:
            raise ValueError(f"Несовместимый исходный тип {name}: {declared_type}")
        if any(value is not None and type(value) not in allowed for value in values):
            raise ValueError(f"Значение {name} не соответствует исходному типу {declared_type}")
        arrays.append(pa.array(values, type=dtype, safe=True))
    return pa.Table.from_arrays(arrays, names=names)
