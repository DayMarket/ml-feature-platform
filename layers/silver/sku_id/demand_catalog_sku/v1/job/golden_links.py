"""Классифицировать известные активные Uzum SKU→golden связи после merge resolution."""

import re

from .golden_graph import _uuid


def prepare_active_links(rows, *, expected_rows, expected_uzum_rows):
    """Проверить весь raw capture; orphan учесть, но не приписывать marketplace."""
    output, count, orphan_meta_link_rows = [], 0, 0
    stream = iter(rows)
    try:
        if any(type(value) is not int or not 0 < value <= 2**63 - 1 for value in (expected_rows, expected_uzum_rows)):
            raise ValueError("Нужны независимые положительные counts всех active и Uzum links")
        for row in stream:
            if not isinstance(row, dict) or set(row) != {"meta_sku_id", "golden_sku_id", "link_provenance",
                                                        "meta_present", "meta_source", "source_sku_id"}:
                raise ValueError("Неверный состав raw active link")
            count += 1
            if count > expected_rows:
                raise ValueError("Raw active links больше независимого source count")
            meta, golden = _uuid(row["meta_sku_id"]), _uuid(row["golden_sku_id"])
            if (not isinstance(row["link_provenance"], str) or type(row["meta_present"]) is not int
                    or row["meta_present"] not in (0, 1)):
                raise ValueError("Неверные provenance/meta_present активной связи")
            if not row["meta_present"]:
                orphan_meta_link_rows += 1
                continue
            if (not isinstance(row["meta_source"], str) or not row["meta_source"].strip()
                    or not isinstance(row["source_sku_id"], str)):
                raise ValueError("Нет marketplace/SKU identity активной связи")
            if row["meta_source"] != "uzum":
                continue
            raw = row["source_sku_id"]
            if re.fullmatch(r"[0-9]+", raw) is None or not 0 < int(raw) <= 2**63 - 1:
                raise ValueError("Неверный source_sku_id Uzum")
            output.append({"sku_id": int(raw), "meta_sku_id": meta, "golden_sku_id": golden})
            if len(output) > expected_uzum_rows:
                raise ValueError("Uzum links больше независимого source count")
        if count != expected_rows or len(output) != expected_uzum_rows:
            raise ValueError("Неполный raw active links capture")
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()
    return output, {
        "source_active_link_rows": count,
        "orphan_meta_link_rows": orphan_meta_link_rows,
        "recognized_uzum_link_rows": len(output),
    }


def resolve_sku_links(rows, terminals, *, expected_rows):
    """Отсутствие SKU в результате — не автоматический unmatched без проверки полного источника."""
    if not isinstance(terminals, dict) or not terminals:
        raise ValueError("Нужен полный разрешённый golden-граф")
    for node, terminal in terminals.items():
        if _uuid(node) != node or _uuid(terminal) != terminal or terminals.get(terminal) != terminal:
            raise ValueError("Неверный terminal mapping")
    candidates, missing, seen_pairs = {}, set(), set()
    count = 0
    stream = iter(rows)
    try:
        if type(expected_rows) is not int or not 0 < expected_rows <= 2**63 - 1:
            raise ValueError("Нужен положительный независимый count активных Uzum links")
        for row in stream:
            if not isinstance(row, dict) or set(row) != {"sku_id", "meta_sku_id", "golden_sku_id"}:
                raise ValueError("Неверный состав известной Uzum связи")
            sku = row["sku_id"]
            if type(sku) is not int or not 0 < sku <= 2**63 - 1:
                raise ValueError("Неверный SKU активной связи")
            meta, golden = _uuid(row["meta_sku_id"]), _uuid(row["golden_sku_id"])
            count += 1
            if count > expected_rows:
                raise ValueError("Связей больше независимого source count")
            seen_pairs.add((sku, meta, golden))
            choices = candidates.setdefault(sku, set())
            if golden not in terminals:
                missing.add(sku)
            else:
                choices.add(terminals[golden])
        if count != expected_rows:
            raise ValueError("Неполный capture активных связей")
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()
    result = {}
    for sku, choices in candidates.items():
        status = "unavailable" if sku in missing else "conflict" if len(choices) > 1 else "matched"
        golden = next(iter(choices)) if status == "matched" else None
        result[sku] = {"golden_sku_id": golden, "golden_mapping_status": status,
                       "unit_id": f"g:{golden}" if golden is not None else f"s:{sku}" if status == "conflict" else None}
    audit = {"active_uzum_link_rows": count, "duplicate_relation_rows": count - len(seen_pairs),
             "linked_sku": len(result), "sku_with_missing_golden": len(missing),
             **{f"{status}_sku": sum(row["golden_mapping_status"] == status for row in result.values())
                for status in ("matched", "unavailable", "conflict")}}
    return result, audit
