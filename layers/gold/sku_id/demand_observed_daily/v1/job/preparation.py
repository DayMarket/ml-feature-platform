"""Соединить продажи и разреженное наличие без модельных окон."""

from contextlib import ExitStack
from datetime import date, datetime, timezone
import re
from uuid import UUID

import pyarrow as pa
import pyarrow.compute as pc

KEYS = ("date", "sku_id")
SALES_SHARED = [
    "source_updated_at",
    "fx_rate_date",
    "fx_rate_uzs_per_usd",
    "fx_rate_source",
    "fx_captured_at",
    "source_manifest_id",
    "source_contract_version",
    "ingested_at",
]
SALES_REQUIRED = [
    "source_updated_at",
    "fx_rate_source",
    "fx_captured_at",
    "source_manifest_id",
    "source_contract_version",
    "ingested_at",
]
SOURCE_FIELDS = {
    "sales": [
        "date",
        "sku_id",
        "sales_units",
        "sales_order_items",
        "sales_orders",
        "sales_gmv",
        "sales_payment_value",
        "sales_full_value",
        "sales_seller_promo_value",
        "sales_marketplace_promo_value",
        "sales_gmv_fbo",
        "sales_gmv_fbs",
        "sales_gmv_dbs",
        "sales_gmv_other",
        "sales_gmv_unknown",
        "sales_units_fbo",
        "sales_units_fbs",
        "sales_units_dbs",
        "sales_units_other",
        "sales_units_unknown",
        "sales_gmv_usd",
        "sales_payment_value_usd",
        "sales_full_value_usd",
        "sales_seller_promo_value_usd",
        "sales_marketplace_promo_value_usd",
        "sales_gmv_fbo_usd",
        "sales_gmv_fbs_usd",
        "sales_gmv_dbs_usd",
        "sales_gmv_other_usd",
        "sales_gmv_unknown_usd",
        *SALES_SHARED,
    ],
    "stock": [
        "date",
        "sku_id",
        "source_manifest_id",
        "source_contract_version",
        "ingested_at",
    ],
}
EXPECTED = {
    "date": pa.date32(),
    "sku_id": pa.int64(),
    "sales_units": pa.int64(),
    "sales_order_items": pa.int64(),
    "sales_orders": pa.int64(),
    "sales_gmv": pa.decimal128(38, 0),
    "sales_payment_value": pa.decimal128(38, 0),
    "sales_full_value": pa.decimal128(38, 0),
    "sales_seller_promo_value": pa.decimal128(38, 0),
    "sales_marketplace_promo_value": pa.decimal128(38, 0),
    "sales_gmv_fbo": pa.decimal128(38, 0),
    "sales_gmv_fbs": pa.decimal128(38, 0),
    "sales_gmv_dbs": pa.decimal128(38, 0),
    "sales_gmv_other": pa.decimal128(38, 0),
    "sales_gmv_unknown": pa.decimal128(38, 0),
    "sales_units_fbo": pa.int64(),
    "sales_units_fbs": pa.int64(),
    "sales_units_dbs": pa.int64(),
    "sales_units_other": pa.int64(),
    "sales_units_unknown": pa.int64(),
    "sales_gmv_usd": pa.float64(),
    "sales_payment_value_usd": pa.float64(),
    "sales_full_value_usd": pa.float64(),
    "sales_seller_promo_value_usd": pa.float64(),
    "sales_marketplace_promo_value_usd": pa.float64(),
    "sales_gmv_fbo_usd": pa.float64(),
    "sales_gmv_fbs_usd": pa.float64(),
    "sales_gmv_dbs_usd": pa.float64(),
    "sales_gmv_other_usd": pa.float64(),
    "sales_gmv_unknown_usd": pa.float64(),
    "sales_source_updated_at": pa.timestamp("us"),
    "sales_fx_rate_date": pa.date32(),
    "sales_fx_rate_uzs_per_usd": pa.float64(),
    "sales_fx_rate_source": pa.string(),
    "sales_fx_captured_at": pa.timestamp("us"),
    "sales_source_manifest_id": pa.string(),
    "sales_source_contract_version": pa.string(),
    "sales_ingested_at": pa.timestamp("us"),
    "sales_component_present": pa.bool_(),
    "sales_snapshot_id": pa.int64(),
    "sales_table_uuid": pa.string(),
    "is_in_stock_eod": pa.bool_(),
    "stock_snapshot_id": pa.int64(),
    "stock_table_uuid": pa.string(),
    "source_manifest_id": pa.string(),
    "source_contract_version": pa.string(),
    "ingested_at": pa.timestamp("us"),
}
REQUIRED = [
    "date",
    "sku_id",
    "sales_component_present",
    "sales_snapshot_id",
    "sales_table_uuid",
    "is_in_stock_eod",
    "stock_snapshot_id",
    "stock_table_uuid",
    "source_manifest_id",
    "source_contract_version",
    "ingested_at",
]


def target_ref(config, catalog_name):
    table = config["table"]
    if any(
        not isinstance(table.get(key), str)
        or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table[key])
        for key in ("catalog", "schema", "name")
    ) or table["catalog"] != catalog_name:
        raise ValueError("Неверный Iceberg идентификатор gold")
    return table["schema"], table["name"]


def output_name(name):
    return f"sales_{name}" if name in SALES_SHARED else name


def same_type(actual, expected):
    return actual == expected or pa.types.is_string(expected) and pa.types.is_large_string(actual)


def validate_schema(schema):
    if len(schema) != len(EXPECTED) or set(schema.names) != set(EXPECTED):
        raise ValueError("Не совпадает набор колонок observed migration")
    for name, dtype in EXPECTED.items():
        field = schema.field(name)
        if not same_type(field.type, dtype) or field.nullable != (name not in REQUIRED):
            raise ValueError(f"Неверный тип или nullable observed.{name}")


def source_type(kind, name):
    if kind == "sales":
        return EXPECTED[output_name(name)]
    return {
        "date": pa.date32(),
        "sku_id": pa.int64(),
        "source_manifest_id": pa.string(),
        "source_contract_version": pa.string(),
        "ingested_at": pa.timestamp("us"),
    }[name]


def validate_source_schema(kind, schema):
    fields = SOURCE_FIELDS[kind]
    if len(schema) != len(fields) or set(schema.names) != set(fields):
        raise ValueError(f"Неверная схема входа {kind}")
    for name in fields:
        if not same_type(schema.field(name).type, source_type(kind, name)):
            raise ValueError(f"Неверный тип входа {kind}.{name}")


def source_batches(batches, kind, day, size):
    """Проверить дневные ключи и обязательные поля целыми Arrow-колонками."""
    previous = None
    required = SALES_REQUIRED if kind == "sales" else SOURCE_FIELDS["stock"][2:]
    for batch in batches:
        if not isinstance(batch, pa.Table):
            raise ValueError("Компонент должен поступать Arrow-порциями")
        validate_source_schema(kind, batch.schema)
        if not batch.num_rows:
            continue
        ids = batch["sku_id"]
        if (batch["date"].null_count or ids.null_count
                or pc.any(pc.not_equal(batch["date"], day)).as_py()
                or pc.min(ids).as_py() <= 0
                or previous is not None and ids[0].as_py() <= previous
                or batch.num_rows > 1 and pc.any(
                    pc.less_equal(ids.slice(1), ids.slice(0, batch.num_rows - 1))).as_py()):
            raise ValueError(f"Неверный день/порядок/дубли SKU во входе {kind}")
        if any(batch[name].null_count for name in required):
            raise ValueError(f"NULL обязательного поля входа {kind}")
        previous = ids[-1].as_py()
        for offset in range(0, batch.num_rows, size):
            yield batch.slice(offset, size)


def metadata(inputs, manifest, version, ingested_at):
    if set(inputs) != {"sales", "stock"}:
        raise ValueError("Нужны обе точные версии sales/stock")
    if (
        not isinstance(ingested_at, datetime)
        or ingested_at.utcoffset() is None
        or any(not isinstance(value, str) or not value.strip() for value in (manifest, version))
    ):
        raise ValueError("Нужны manifest/version и timezone-aware ingestion")
    result = {
        "source_manifest_id": manifest,
        "source_contract_version": version,
        "ingested_at": ingested_at.astimezone(timezone.utc).replace(tzinfo=None),
    }
    for kind, source in inputs.items():
        snapshot = source.get("snapshot_id")
        table_uuid = source.get("table_uuid")
        if type(snapshot) is not int or snapshot <= 0 or not isinstance(table_uuid, str):
            raise ValueError(f"Нужны snapshot_id/table_uuid входа {kind}")
        try:
            UUID(table_uuid)
        except ValueError as error:
            raise ValueError(f"Неверный UUID входа {kind}") from error
        result[f"{kind}_snapshot_id"] = snapshot
        result[f"{kind}_table_uuid"] = table_uuid
    return result


def validate_batch(batch, schema, *, day):
    validate_schema(schema)
    if not isinstance(batch, pa.Table) or not batch.schema.equals(schema, check_metadata=False):
        raise ValueError("Observed batch не соответствует Iceberg schema")
    if any(batch[name].null_count for name in REQUIRED):
        raise ValueError("NULL обязательного поля observed")
    if not batch.num_rows:
        return batch
    ids = batch["sku_id"]
    if (pc.any(pc.not_equal(batch["date"], day)).as_py() or pc.min(ids).as_py() <= 0
            or batch.num_rows > 1 and pc.any(
                pc.less_equal(ids.slice(1), ids.slice(0, batch.num_rows - 1))).as_py()):
        raise ValueError("Неверный день/порядок/дубли observed")
    present = batch["sales_component_present"]
    absent = pc.invert(present)
    if pc.any(pc.invert(pc.or_(present, batch["is_in_stock_eod"]))).as_py():
        raise ValueError("Строка observed без продаж и наличия")
    for name in SOURCE_FIELDS["sales"]:
        if name not in KEYS and pc.any(pc.and_(absent, pc.is_valid(batch[output_name(name)]))).as_py():
            raise ValueError("Отсутствующий sales-компонент должен оставаться NULL")
    for name in SALES_REQUIRED:
        if pc.any(pc.and_(present, pc.is_null(batch[output_name(name)]))).as_py():
            raise ValueError("NULL обязательного поля sales-компонента")
    if pc.any(pc.greater(batch["sales_ingested_at"], batch["ingested_at"])).as_py():
        raise ValueError("Некорректное время захвата sales")
    return batch


def upper_bound(ids, boundary):
    """Найти общий префикс двух отсортированных порций без Python-массива ключей."""
    low, high = 0, len(ids)
    while low < high:
        middle = (low + high) // 2
        if ids[middle].as_py() <= boundary:
            low = middle + 1
        else:
            high = middle
    return low


def join_batches(sales, stock, schema, *, day, inputs, manifest, version, ingested_at, max_batch_rows):
    """Arrow full join согласованных префиксов: память ограничена размером порций."""
    validate_schema(schema)
    if type(day) is not date or type(max_batch_rows) is not int or max_batch_rows <= 0:
        raise ValueError("Нужны DATE и положительный лимит порции")
    meta = metadata(inputs, manifest, version, ingested_at)
    with ExitStack() as stack:
        streams, empty = {}, {}
        for kind, batches in (("sales", sales), ("stock", stock)):
            batches = iter(batches)
            if callable(getattr(batches, "close", None)):
                stack.callback(batches.close)
            streams[kind] = iter(source_batches(batches, kind, day, max_batch_rows))
            empty[kind] = pa.schema(
                [pa.field(name, source_type(kind, name)) for name in SOURCE_FIELDS[kind]]
            ).empty_table()
        current = {kind: next(stream, None) for kind, stream in streams.items()}
        pending, count = [], 0
        while any(table is not None for table in current.values()):
            boundary = min(table["sku_id"][-1].as_py() for table in current.values() if table is not None)
            pieces = {}
            for kind, table in current.items():
                length = upper_bound(table["sku_id"], boundary) if table is not None else 0
                pieces[kind] = table.slice(0, length) if table is not None else empty[kind]
                if table is not None:
                    current[kind] = table.slice(length)
            left = pieces["sales"]
            left = left.rename_columns([output_name(name) for name in left.column_names])
            left = left.append_column("sales_component_present", pa.repeat(True, left.num_rows))
            right = pieces["stock"].select(KEYS)
            right = right.append_column("is_in_stock_eod", pa.repeat(True, right.num_rows))
            result = left.join(right, keys=list(KEYS), join_type="full outer")
            for name in ("sales_component_present", "is_in_stock_eod"):
                result = result.set_column(
                    result.schema.get_field_index(name), name, pc.fill_null(result[name], False))
            for name, value in meta.items():
                result = result.append_column(name, pa.repeat(pa.scalar(value, type=schema.field(name).type),
                                                              result.num_rows))
            result = result.select(schema.names).cast(schema, safe=True).sort_by([("sku_id", "ascending")])
            offset = 0
            while offset < result.num_rows:
                length = min(max_batch_rows - count, result.num_rows - offset)
                pending.append(result.slice(offset, length))
                offset, count = offset + length, count + length
                if count == max_batch_rows:
                    yield validate_batch(pa.concat_tables(pending).combine_chunks(), schema, day=day)
                    pending, count = [], 0
            for kind, table in current.items():
                if table is not None and not table.num_rows:
                    current[kind] = next(streams[kind], None)
        if pending:
            yield validate_batch(pa.concat_tables(pending).combine_chunks(), schema, day=day)
