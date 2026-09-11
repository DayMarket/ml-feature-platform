"""Нормализовать полный category capture без потери raw уровней и конфликтов."""

LEVELS = ("market", "l1", "l2", "l3", "l4", "l5", "leaf")
RAW_LEVELS = tuple(f"raw_l{level}_category_id" for level in range(1, 7))
CATEGORY_FIELDS = ("category_id", *RAW_LEVELS, "l1_title", "leaf_title")


def missing_category():
    return {**dict.fromkeys((*RAW_LEVELS, *LEVELS, "l1_title", "leaf_title")), "category_path_status": "missing"}


def build_category_index(rows, *, expected_rows):
    """Конфликт пути остаётся явным статусом; SKU writer обязан блокировать такие строки."""
    index, parents = {}, {}
    stream = iter(rows)
    try:
        if type(expected_rows) is not int or not 0 < expected_rows <= 2**63 - 1:
            raise ValueError("Нужен положительный независимый count категорий")
        for row in stream:
            if not isinstance(row, dict) or set(row) != set(CATEGORY_FIELDS):
                raise ValueError("Неверный состав category capture")
            category_id = row["category_id"]
            if type(category_id) is not int or not 0 < category_id <= 2**63 - 1 or category_id in index:
                raise ValueError("Неверный или повторный category_id")
            if any(type(row[name]) is not int or not 0 <= row[name] <= 2**63 - 1 for name in RAW_LEVELS):
                raise ValueError("Raw уровни category capture требуют UInt64 в signed BIGINT диапазоне")
            if any(not isinstance(row[name], str) for name in ("l1_title", "leaf_title")):
                raise ValueError("Названия присутствующей категории должны быть строками")
            result = {**missing_category(), **{name: row[name] for name in (*RAW_LEVELS, "l1_title", "leaf_title")}}
            if row[RAW_LEVELS[0]] > 0:
                path = [row[RAW_LEVELS[0]]]
                for name in RAW_LEVELS[1:5]:
                    path.append(row[name] or path[-1])
                path.append(category_id)
                result.update(market="market", category_path_status="valid")
                result.update({level: f"{level}:{number}" for level, number in zip(LEVELS[1:], path, strict=True)})
                for position, level in enumerate(LEVELS[1:], start=1):
                    parents.setdefault(result[level], set()).add(result[LEVELS[position - 1]])
            index[category_id] = result
            if len(index) > expected_rows:
                raise ValueError("Категорий больше независимого source count")
        if len(index) != expected_rows:
            raise ValueError("Неполный category capture")
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()
    conflicts = {node for node, choices in parents.items() if len(choices) > 1}
    for result in index.values():
        if any(result[level] in conflicts for level in LEVELS):
            result.update(dict.fromkeys(LEVELS))
            result["category_path_status"] = "conflict"
    audit = {"category_rows": len(index), "conflicting_nodes": len(conflicts),
             **{f"{status}_category_rows": sum(row["category_path_status"] == status for row in index.values())
                for status in ("valid", "missing", "conflict")}}
    return index, audit
