"""Извлечь raw SKU/category/MDM captures без подмены source semantics."""

import re


def source_ref(config, key):
    value = config["source"].get(key)
    match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)", value or "") if isinstance(value, str) else None
    if match is None:
        raise ValueError(f"Нужно отдельное database.table имя source.{key}")
    return f"`{match[1]}`.`{match[2]}`"


def capture_query(config, kind, *, metadata_only=False):
    """active_links включает orphan meta для явного DQ-учёта до marketplace-фильтра."""
    if type(metadata_only) is not bool:
        raise ValueError("Нужен Boolean metadata_only")
    if kind == "sku":
        fields = ("id AS sku_id, product_id, category_id, seller_id, shop_id, "
                  "toDateTime64(toTimeZone(created_at, 'UTC'), 6, 'UTC') AS sku_created_at, status AS sku_status")
        source, where, order = source_ref(config, "sku"), "", "sku_id"
    elif kind == "category":
        fields = "id AS category_id, " + ", ".join(f"l{n}_category AS raw_l{n}_category_id" for n in range(1, 7))
        fields += ", l1_category_title AS l1_title, title_ru AS leaf_title"
        source, where, order = source_ref(config, "category"), "", "category_id"
    elif kind == "golden":
        fields = "golden_sku_id, is_merged, merged_into"
        source, where, order = source_ref(config, "golden") + " FINAL", "", "golden_sku_id"
    elif kind == "active_links":
        source_ref(config, "meta_sku")
        dictionary = config["source"]["meta_sku"]
        has_meta = f"dictHas('{dictionary}', meta_sku_id)"
        fields = "meta_sku_id, golden_sku_id, source AS link_provenance, " + has_meta + " AS meta_present"
        for attr, alias in (("source", "meta_source"), ("source_sku_id", "source_sku_id")):
            fields += (f", if({has_meta}, dictGetString('{dictionary}', '{attr}', meta_sku_id), "
                       f"CAST(NULL AS Nullable(String))) AS {alias}")
        source, where, order = source_ref(config, "golden_links"), " WHERE deleted_at IS NULL", "meta_sku_id, golden_sku_id, link_provenance"
    else:
        raise ValueError("Неизвестный вид source capture")
    tail = " LIMIT 0" if metadata_only else f" ORDER BY {order}"
    return f"SELECT {fields} FROM {source}{where}{tail} SETTINGS max_threads=1, max_execution_time=300"
