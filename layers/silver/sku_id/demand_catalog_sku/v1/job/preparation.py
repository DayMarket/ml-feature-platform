"""Собрать полный SKU-каталог колоночным JOIN проверенных source captures."""

from datetime import datetime, timezone
import re
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

from .category_paths import LEVELS, RAW_LEVELS, build_category_index
from .golden_graph import resolve_golden_graph
from .golden_links import prepare_active_links, resolve_sku_links

SOURCE_FIELDS = ("sku_id", "product_id", "category_id", "seller_id", "shop_id", "sku_created_at", "sku_status")
SELLER_FIELDS = ("source_master_seller_id", "master_seller_id", "seller_mapping_status", "has_master",
                 "is_1p", "seller_registered_at")


def target_ref(config, catalog_name):
    table = config["table"]
    if any(not isinstance(table.get(key), str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table[key])
           for key in ("catalog", "schema", "name")):
        raise ValueError("Нужны отдельные компоненты Iceberg identifier")
    if table["catalog"] != catalog_name:
        raise ValueError("Другой Iceberg каталог")
    return table["schema"], table["name"]


def expected_schema():
    fields = [("date", pa.date32(), False), ("sku_id", pa.int64(), False)]
    fields += [(name, pa.int64(), True) for name in SOURCE_FIELDS[1:5]]
    fields += [("sku_created_at", pa.timestamp("us"), True), ("sku_status", pa.string(), True)]
    fields += [(name, pa.int64(), True) for name in RAW_LEVELS]
    fields += [(name, pa.string(), True) for name in (*LEVELS, "l1_title", "leaf_title")]
    fields += [("category_path_status", pa.string(), False), ("golden_sku_id", pa.string(), True),
               ("golden_mapping_status", pa.string(), False), ("unit_id", pa.string(), True)]
    fields += [(name, pa.string(), name != "seller_mapping_status") for name in SELLER_FIELDS[:3]]
    fields += [("has_master", pa.bool_(), True), ("is_1p", pa.bool_(), True),
               ("seller_registered_at", pa.timestamp("us"), True), ("catalog_version", pa.string(), False),
               ("catalog_seller_snapshot_id", pa.int64(), False), ("source_contract_version", pa.string(), False),
               ("source_manifest_id", pa.string(), False), ("ingested_at", pa.timestamp("us"), False)]
    return pa.schema([pa.field(name, kind, nullable=nullable) for name, kind, nullable in fields])


def validate_schema(schema):
    expected = expected_schema()
    if len(schema) != len(expected) or set(schema.names) != set(expected.names):
        raise ValueError("SKU-каталог требует 38 согласованных полей")
    for field in expected:
        found = schema.field(field.name)
        same = found.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(found.type)
        if not same or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable SKU.{field.name}")


def _positive(value, name):
    if type(value) is not int or not 0 < value <= 2**63 - 1:
        raise ValueError(f"Нужен независимый положительный {name}")
    return value


def _all_equal(column, value):
    return not column.null_count and pc.all(pc.equal(column, pa.scalar(value, type=column.type))).as_py() is True


def _seller_source(seller, bound, captured_at):
    """DQ/UUID/schema binding выполняет inputs; здесь сверяется полный payload этого snapshot."""
    receipt = bound["receipt"]
    names = ("date", "seller_id", *SELLER_FIELDS, "catalog_version", "source_contract_version",
             "source_manifest_id", "ingested_at")
    if (not isinstance(seller, pa.Table) or len(seller.column_names) != len(names)
            or set(seller.column_names) != set(names)
            or seller.num_rows != _positive(receipt["rows_written"], "seller count")):
        raise ValueError("Нужен полный seller snapshot с 12 полями")
    if not isinstance(bound["captured_at"], datetime) or bound["captured_at"].utcoffset() is None:
        raise ValueError("Нужно aware время seller capture")
    if (bound["captured_at"] > captured_at
            or bound["captured_at"].astimezone(ZoneInfo("Asia/Tashkent")).date() != bound["date"]):
        raise ValueError("Seller capture не соответствует времени материализации")
    types = {"date": pa.date32(), "seller_id": pa.int64(), "has_master": pa.bool_(), "is_1p": pa.bool_(),
             "seller_registered_at": pa.timestamp("us"), "ingested_at": pa.timestamp("us")}
    for name in names:
        actual, expected = seller[name].type, types.get(name, pa.string())
        if actual != expected and not (pa.types.is_string(expected) and pa.types.is_large_string(actual)):
            raise ValueError(f"Неверный тип seller.{name}")
    for name, value in {"date": bound["date"], "catalog_version": receipt["catalog_version"],
                        "source_contract_version": receipt["source_contract_version"],
                        "source_manifest_id": receipt["source_manifest_id"],
                        "ingested_at": bound["captured_at"].astimezone(timezone.utc).replace(tzinfo=None)}.items():
        if not _all_equal(seller[name], value):
            raise ValueError(f"Seller snapshot содержит чужой {name}")
    ids = seller["seller_id"]
    if ids.null_count or pc.min(ids).as_py() <= 0 or pc.count_distinct(ids).as_py() != seller.num_rows:
        raise ValueError("Неверные/повторные seller_id")
    # Проверяется небольшой seller-справочник, не миллионы строк SKU.
    actual_masters, fallback_ids = set(), set()
    for sid, raw, master, status, has in zip(*(seller[name].to_pylist() for name in
                                              ("seller_id", *SELLER_FIELDS[:4])), strict=True):
        if raw is None:
            expected = (None, "unavailable", None)
        elif raw.strip():
            expected = (raw.strip(), "matched", True)
            actual_masters.add(raw.strip())
        else:
            expected = (str(sid), "unmatched", False)
            fallback_ids.add(str(sid))
        if (master, status, has) != expected:
            raise ValueError("Seller master/status не соответствует raw значению")
    if actual_masters & fallback_ids:
        raise ValueError("Коллизия master и fallback seller_id")
    return seller


def prepare_catalog(source, schema, *, categories, goldens, active_links, counts, seller, bound_seller,
                    source_manifest_id, source_contract_version, ingested_at):
    """Полные raw captures обязательны; отсутствие MDM-связи становится unmatched лишь после их проверок."""
    validate_schema(schema)
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("Нужно aware время захвата")
    receipt = bound_seller["receipt"]
    catalog_version = receipt["catalog_version"]
    for value in (catalog_version, source_manifest_id, source_contract_version):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Нужны версии и source manifest")
    snapshot_id = _positive(receipt["snapshot_id"], "seller snapshot ID")
    if not isinstance(counts, dict) or set(counts) != {"sku", "category", "golden", "active_links", "uzum_links"}:
        raise ValueError("Нужны counts всех raw captures")
    for name, value in counts.items():
        _positive(value, name)
    if (not isinstance(source, pa.Table) or source.num_rows != counts["sku"]
            or len(source.column_names) != len(SOURCE_FIELDS) or set(source.column_names) != set(SOURCE_FIELDS)):
        raise ValueError("Нужен полный raw SKU capture с семью полями")
    values = {}
    for name in SOURCE_FIELDS[:5]:
        col = source[name]
        if not pa.types.is_integer(col.type):
            raise ValueError(f"Нужен целочисленный raw {name}")
        col = col.cast(pa.int64(), safe=True)
        if (name == "sku_id" and col.null_count) or (pc.min(col).as_py() is not None and pc.min(col).as_py() < (1 if name == "sku_id" else 0)):
            raise ValueError(f"Неверный raw {name}")
        values[name] = col
    ids = values["sku_id"]
    if source.num_rows > 1 and pc.any(pc.less_equal(ids.slice(1), ids.slice(0, source.num_rows - 1))).as_py():
        raise ValueError("Raw SKU должны быть строго упорядочены без повторов")
    if source["sku_created_at"].type != pa.timestamp("us", "UTC"):
        raise ValueError("sku_created_at должен быть явно UTC в микросекундах")
    if not (pa.types.is_string(source["sku_status"].type) or pa.types.is_large_string(source["sku_status"].type)):
        raise ValueError("sku_status должен быть raw строкой/NULL")
    values.update(sku_created_at=source["sku_created_at"].cast(pa.timestamp("us")), sku_status=source["sku_status"])
    seller = _seller_source(seller, bound_seller, ingested_at)
    category_index, category_audit = build_category_index(categories, expected_rows=counts["category"])
    terminals, graph_audit = resolve_golden_graph(goldens, expected_rows=counts["golden"])
    links, link_capture_audit = prepare_active_links(
        active_links,
        expected_rows=counts["active_links"],
        expected_uzum_rows=counts["uzum_links"],
    )
    golden_index, links_audit = resolve_sku_links(
        links,
        terminals,
        expected_rows=counts["uzum_links"],
    )
    links_audit = link_capture_audit | links_audit
    dimensions = (("category_id", category_index), ("sku_id", golden_index))
    for key, index in dimensions:
        records = [{key: ident, **record} for ident, record in index.items()]
        field_names = (key, *next(iter(index.values())))
        lookup_schema = pa.schema([schema.field(name).with_nullable(True) for name in field_names])
        dimension = pa.Table.from_pylist(records, schema=lookup_schema)
        positions = pc.index_in(values[key], value_set=dimension[key])
        for name in field_names[1:]:
            values[name] = pc.take(dimension[name], positions)
    values["category_path_status"] = pc.fill_null(values["category_path_status"], "missing")
    values["golden_mapping_status"] = pc.fill_null(values["golden_mapping_status"], "unmatched")
    fallback = pc.binary_join_element_wise("s:", ids.cast(pa.string()), "")
    standalone = pc.is_in(
        values["golden_mapping_status"],
        value_set=pa.array(["unmatched", "conflict"], type=values["golden_mapping_status"].type),
    )
    values["unit_id"] = pc.if_else(standalone, fallback, values["unit_id"])
    positions = pc.index_in(values["seller_id"], value_set=seller["seller_id"])
    for name in SELLER_FIELDS:
        values[name] = pc.take(seller[name], positions)
    values["seller_mapping_status"] = pc.fill_null(values["seller_mapping_status"], "unavailable")
    for name, allowed in (("category_path_status", ["valid", "missing"]),
                          ("golden_mapping_status", ["matched", "unmatched", "conflict"]),
                          ("seller_mapping_status", ["matched", "unmatched"])):
        if not pc.all(pc.is_in(values[name], value_set=pa.array(allowed, type=values[name].type))).as_py():
            raise ValueError(f"Запись запрещена: {name} содержит conflict/unavailable")
    constants = {"date": ingested_at.astimezone(ZoneInfo("Asia/Tashkent")).date(),
                 "catalog_version": catalog_version, "catalog_seller_snapshot_id": snapshot_id,
                 "source_manifest_id": source_manifest_id, "source_contract_version": source_contract_version,
                 "ingested_at": ingested_at.astimezone(timezone.utc).replace(tzinfo=None)}
    arrays = [pa.repeat(pa.scalar(constants[field.name], type=field.type), source.num_rows)
              if field.name in constants else values[field.name].cast(field.type, safe=True) for field in schema]
    output = pa.Table.from_arrays(arrays, schema=schema)
    output.validate(full=True)
    if any(not field.nullable and output[field.name].null_count for field in schema):
        raise ValueError("NULL в обязательном поле SKU-каталога")
    audit = {"source_sku_rows": source.num_rows, "seller_rows": seller.num_rows, "output_bytes": output.nbytes,
             "category": category_audit, "golden_graph": graph_audit, "golden_links": links_audit}
    for name in ("category_path_status", "golden_mapping_status", "seller_mapping_status"):
        audit[name] = {row["values"]: row["counts"] for row in pc.value_counts(output[name]).to_pylist()}
    return output, audit
