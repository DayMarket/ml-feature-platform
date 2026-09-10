"""Разрешить все MDM merge-цепочки одного полного захваченного golden-графа."""

from uuid import UUID


def _uuid(value, *, allow_zero=False):
    if not isinstance(value, (str, UUID)):
        raise ValueError("Golden ID должен быть UUID")
    try:
        result = UUID(value) if isinstance(value, str) else value
    except ValueError as error:
        raise ValueError("Невалидный golden UUID") from error
    if not allow_zero and result.int == 0:
        raise ValueError("Нулевой golden UUID")
    return str(result)


def resolve_golden_graph(rows, *, expected_rows):
    """Вернуть terminal ID для всех golden; никаких запросов и выбора первого дубля."""
    edges = {}
    stream = iter(rows)
    try:
        if type(expected_rows) is not int or not 0 < expected_rows <= 2**63 - 1:
            raise ValueError("Нужен положительный независимый count полного MDM-графа")
        for row in stream:
            if not isinstance(row, dict) or set(row) != {"golden_sku_id", "is_merged", "merged_into"}:
                raise ValueError("Нужны golden_sku_id/is_merged/merged_into")
            node = _uuid(row["golden_sku_id"])
            flag = row["is_merged"]
            if type(flag) is not int or flag not in (0, 1):
                raise ValueError("is_merged должен быть UInt8 0 или 1")
            target = _uuid(row["merged_into"], allow_zero=flag == 0)
            if node in edges:
                raise ValueError("Повтор golden ID, произвольный dedup запрещён")
            edges[node] = target if flag else None
            if len(edges) > expected_rows:
                raise ValueError("MDM-граф больше независимого source count")
        if len(edges) != expected_rows:
            raise ValueError("Неполный MDM-граф")
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()
    if any(target is not None and target not in edges for target in edges.values()):
        raise ValueError("В MDM-графе отсутствует цель merge")
    resolved, hops = {}, {}
    for start in edges:
        if start in resolved:
            continue
        path, visiting = [], set()
        node = start
        while node not in resolved:
            if node in visiting:
                raise ValueError("Цикл в MDM merge-графе")
            visiting.add(node)
            target = edges[node]
            if target is None:
                resolved[node], hops[node] = node, 0
                break
            path.append(node)
            node = target
        terminal, depth = resolved[node], hops[node]
        for ancestor in reversed(path):
            depth += 1
            resolved[ancestor], hops[ancestor] = terminal, depth
    audit = {"golden_rows": len(edges), "merged_rows": sum(target is not None for target in edges.values()),
             "terminal_rows": sum(target is None for target in edges.values()),
             "multi_hop_rows": sum(depth > 1 for depth in hops.values()), "max_merge_hops": max(hops.values())}
    return resolved, audit
