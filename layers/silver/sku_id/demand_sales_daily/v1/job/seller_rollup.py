"""Проверить повторённые exact SKU-day totals перед свёрткой seller-sales."""

import pyarrow as pa
import pyarrow.compute as pc

from datetime import date, timezone
from decimal import Decimal
import math

from .preparation import MONEY, QUANTITIES, TECH, utc_naive, validate_batch, validate_fx, validate_schema

COUNTS = ("sales_orders", "sales_order_items")
KEYS = ("date", "sku_id")


def exact_sku_counts(source):
    """Принять source только после exact snapshot/DQ binding вызывающим runtime."""
    needed = (*KEYS, "seller_key", *COUNTS, *(f"sku_{name}" for name in COUNTS))
    if not isinstance(source, pa.Table) or any(name not in source.column_names for name in needed):
        raise ValueError("Нет полей seller-sales для exact SKU totals")
    if any(source[name].null_count for name in needed):
        raise ValueError("NULL в ключах или обязательных exact счётчиках")
    key_type = source["seller_key"].type
    if not pa.types.is_date32(source["date"].type) or not (pa.types.is_string(key_type) or pa.types.is_large_string(key_type)):
        raise ValueError("Неверные типы date/seller_key")
    if pc.any(pc.invert(pc.match_substring_regex(source["seller_key"], r"^(seller:[1-9][0-9]*|unknown)$"))).as_py():
        raise ValueError("Неверный seller_key")
    for name in ("sku_id", *COUNTS, *(f"sku_{name}" for name in COUNTS)):
        if not pa.types.is_int64(source[name].type):
            raise ValueError(f"Нужен BIGINT: {name}")
        if pc.any(pc.less_equal(source[name], 0)).as_py():
            raise ValueError(f"Неположительный идентификатор/счётчик: {name}")
    if len(source.group_by([*KEYS, "seller_key"]).aggregate([])) != len(source):
        raise ValueError("Повтор seller-ключа внутри SKU-дня")
    for prefix in ("", "sku_"):
        if pc.any(pc.greater(source[f"{prefix}sales_orders"], source[f"{prefix}sales_order_items"])).as_py():
            raise ValueError("Заказов больше, чем позиций")
    aggregates = []
    for name in COUNTS:
        # Сумма seller-счётчиков может превышать BIGINT даже при допустимом distinct SKU.
        source = source.set_column(source.schema.get_field_index(name), name, pc.cast(source[name], pa.decimal128(38, 0)))
        aggregates.extend([(f"sku_{name}", "min"), (f"sku_{name}", "max"),
                           (name, "max"), (name, "sum")])
    grouped = source.group_by(list(KEYS)).aggregate(aggregates)
    output = {name: grouped[name] for name in KEYS}
    for name in COUNTS:
        total = grouped[f"sku_{name}_min"]
        if pc.any(pc.not_equal(total, grouped[f"sku_{name}_max"])).as_py():
            raise ValueError("Разные контрольные итоги продавцов одного SKU-дня")
        if pc.any(pc.or_(pc.less(total, grouped[f"{name}_max"]),
                         pc.greater(total, grouped[f"{name}_sum"]))).as_py():
            raise ValueError("Контрольный итог вне границ объединения seller-множеств")
        output[name] = total
    return pa.table(output).sort_by([(name, "ascending") for name in KEYS])


def validate_seller_schema(source, target):
    validate_schema(target)
    extra = {"seller_key": pa.field("seller_key", pa.string(), nullable=False),
             "seller_id": pa.field("seller_id", pa.int64()),
             "sku_sales_orders": pa.field("sku_sales_orders", pa.int64(), nullable=False),
             "sku_sales_order_items": pa.field("sku_sales_order_items", pa.int64(), nullable=False)}
    expected = {field.name: field for field in target} | extra
    if len(source) != len(expected) or set(source.names) != set(expected):
        raise ValueError("Нужны все 42 поля seller-sales")
    for field in source:
        wanted = expected[field.name]
        same = field.type == wanted.type or pa.types.is_string(wanted.type) and pa.types.is_large_string(field.type)
        if not same or field.nullable != wanted.nullable:
            raise ValueError(f"Неверный тип/nullable seller-sales.{field.name}")


def rollup_batches(batches, schema, *, day, manifest, version, ingested_at,
                   max_batch_rows, max_batch_bytes):
    """Свернуть полный отсортированный поток дня; exact DQ/snapshot проверяет caller."""
    validate_schema(schema)
    captured = utc_naive(ingested_at)
    if type(day) is not date or any(not isinstance(v, str) or not v.strip() for v in (manifest, version)):
        raise ValueError("Нужны DATE и manifest/version SKU-sales")
    if any(type(v) is not int or v <= 0 for v in (max_batch_rows, max_batch_bytes)):
        raise ValueError("Нужны положительные лимиты порций")
    additive = tuple(n for n in QUANTITIES if n not in COUNTS) + MONEY
    usd = tuple(n + "_usd" for n in MONEY)
    previous, common, current, output = None, None, None, []

    def complete(state):
        if any(not state["maximum"][n] <= state["controls"][n] <= state["count_sum"][n] for n in COUNTS):
            raise ValueError("SKU controls вне границ объединения seller-множеств")
        row = dict(state["row"])
        for name in additive:
            value = state["sums"][name]
            row[name] = None if name in state["nulls"] else Decimal(value) if name in MONEY else value
        for name in usd:
            row[name] = None if name in state["nulls"] else math.fsum(state["usd"][name])
        row.update(state["controls"])
        row.update(source_manifest_id=manifest, source_contract_version=version, ingested_at=captured)
        return {field.name: row[field.name] for field in schema}

    def pack(rows):
        result = pa.Table.from_pylist(rows, schema=schema)
        if result.nbytes > max_batch_bytes:
            raise ValueError("Превышен размер выходной SKU-порции")
        return validate_batch(result, schema, day=day)

    for batch in batches:
        if (not isinstance(batch, pa.Table) or batch.num_rows > max_batch_rows
                or batch.nbytes > max_batch_bytes):
            raise ValueError("Неверная/слишком большая входная seller-порция")
        validate_seller_schema(batch.schema, schema)
        for row in batch.to_pylist():
            sku, seller, sid = row["sku_id"], row["seller_key"], row["seller_id"]
            if (row["date"] != day or sku is None or sku <= 0 or seller is None
                    or (sid is None and seller != "unknown")
                    or (sid is not None and (sid <= 0 or seller != f"seller:{sid}"))):
                raise ValueError("Неверный день/ключ/атрибуция seller-sales")
            key = (sku, seller)
            if previous is not None and key <= previous:
                raise ValueError("Дубли/порядок seller-ключей между порциями")
            previous = key
            controls = {name: row[f"sku_{name}"] for name in COUNTS}
            if (any(row[n] is None or row[n] < 0 for n in QUANTITIES)
                    or any(controls[n] is None or not 0 < row[n] <= controls[n] for n in COUNTS)
                    or row["sales_orders"] > row["sales_order_items"]
                    or controls["sales_orders"] > controls["sales_order_items"]):
                raise ValueError("Неверные количества или exact SKU controls")
            signature = tuple(row[n] for n in TECH)
            if (any(row[n] is None for n in ("source_manifest_id", "source_contract_version", "ingested_at", "source_updated_at"))
                    or not row["source_manifest_id"] or not row["source_contract_version"]
                    or row["ingested_at"] > captured):
                raise ValueError("Нет согласованного source capture")
            if common is not None and signature != common:
                raise ValueError("Смешанные source/FX captures внутри дня")
            common = signature
            fx = {"date": day, **{n: row[n] for n in TECH[:4]}}
            if fx["fx_captured_at"] is None:
                raise ValueError("Нет FX capture")
            fx["fx_captured_at"] = fx["fx_captured_at"].replace(tzinfo=timezone.utc)
            validate_fx(fx, day)
            if row["fx_captured_at"] > row["ingested_at"]:
                raise ValueError("FX capture позже source capture")
            for name in MONEY:
                raw, value = row[name], row[name + "_usd"]
                if raw is not None and (not raw.is_finite() or raw < 0 and name != "sales_marketplace_promo_value"):
                    raise ValueError("Неверная исходная сумма seller-sales")
                if raw is None or row["fx_rate_source"] == "unavailable":
                    if value is not None:
                        raise ValueError("USD без исходной суммы/курса")
                elif (value is None or not math.isfinite(value)
                      or not math.isclose(value, float(raw) / row["fx_rate_uzs_per_usd"], rel_tol=1e-12, abs_tol=1e-12)):
                    raise ValueError("USD не совпадает с сохранённым курсом")
            for total in ("sales_units", "sales_gmv"):
                parts = [row[total + "_" + c] for c in ("fbo", "fbs", "dbs", "other", "unknown")]
                if row[total] is None or any(v is None for v in parts) or sum(int(v) for v in parts) != int(row[total]):
                    raise ValueError("Нет полного канального разложения seller-sales")
            if current is None or current["row"]["sku_id"] != sku:
                if current is not None:
                    output.append(complete(current))
                    if len(output) == max_batch_rows:
                        yield pack(output)
                        output = []
                current = {"row": row, "controls": controls, "count_sum": dict.fromkeys(COUNTS, 0),
                           "maximum": dict.fromkeys(COUNTS, 0), "sums": dict.fromkeys(additive, 0),
                           "usd": {n: [0.0, 0.0] for n in usd}, "nulls": set()}
            elif current["controls"] != controls:
                raise ValueError("Разные exact controls продавцов одного SKU")
            current["row"]["source_updated_at"] = max(current["row"]["source_updated_at"], row["source_updated_at"])
            for name in COUNTS:
                current["count_sum"][name] += row[name]
                current["maximum"][name] = max(current["maximum"][name], row[name])
            for name in additive:
                if row[name] is None:
                    current["nulls"].add(name)
                else:
                    current["sums"][name] += int(row[name])
            for name in usd:
                value = row[name]
                if value is None:
                    current["nulls"].add(name)
                    continue
                # Компенсированная сумма хранит два числа, не список всех seller-вкладов.
                total, correction = current["usd"][name]
                updated = total + value
                correction += ((total - updated) + value if abs(total) >= abs(value) else (value - updated) + total)
                current["usd"][name] = [updated, correction]
    if current is None:
        raise ValueError("Пустой seller-день не является известным нулём продаж")
    output.append(complete(current))
    if output:
        yield pack(output)
