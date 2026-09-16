"""Колоночное разрешение категорийных конфликтов и MDM merge-цепочек."""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .query import LEVELS

MAX_POINTER_JUMPS = 64


def mark_category_conflicts(categories: pa.Table) -> pa.Table:
    """Узел с несколькими родителями делает конфликтными все пути через него."""
    edges = pa.concat_tables([
        pa.table({
            "node": pc.cast(categories[child], pa.string()),
            "parent": pc.cast(categories[parent], pa.string()),
        })
        for parent, child in zip(LEVELS, LEVELS[1:])
    ])
    edges = edges.filter(pc.is_valid(edges["node"]))
    parents = edges.group_by("node").aggregate([("parent", "count_distinct")])
    conflicts = pc.filter(parents["node"], pc.greater(parents["parent_count_distinct"], 1))
    if len(conflicts) == 0:
        return categories
    conflicted = None
    for level in LEVELS[1:]:
        hit = pc.fill_null(pc.is_in(categories[level], value_set=conflicts), False)
        conflicted = hit if conflicted is None else pc.or_(conflicted, hit)
    result = categories
    for level in LEVELS:
        column = result[level]
        index = result.schema.get_field_index(level)
        result = result.set_column(index, level, pc.if_else(conflicted, pa.scalar(None, column.type), column))
    index = result.schema.get_field_index("category_path_status")
    return result.set_column(
        index, "category_path_status",
        pc.if_else(conflicted, "conflict", result["category_path_status"]),
    )


def resolve_terminals(golden: pa.Table) -> tuple[pa.ChunkedArray, np.ndarray]:
    """Для каждого golden ID — индекс конечного (не слитого) golden, либо -1 для циклов."""
    ids = golden["golden_sku_id"]
    if pc.count_distinct(ids).as_py() != len(ids):
        raise ValueError("Повтор golden_sku_id в текущем состоянии MDM")
    merged = pc.equal(golden["is_merged"], 1).to_numpy(zero_copy_only=False)
    target = pc.index_in(golden["merged_into"], value_set=ids)
    target = pc.fill_null(target, -1).to_numpy(zero_copy_only=False)
    missing = merged & (target < 0)
    if missing.any():
        raise ValueError(f"{int(missing.sum())} merge-целей отсутствуют в MDM-графе")
    parent = np.where(merged, target, np.arange(len(ids)))
    for _ in range(MAX_POINTER_JUMPS):
        following = parent[parent]
        if np.array_equal(following, parent):
            break
        parent = following
    # Цепочка, упирающаяся в цикл, не доходит до не-слитого узла.
    terminal = np.where(merged[parent], -1, parent)
    return ids, terminal


def resolve_sku_links(links: pa.Table, golden: pa.Table) -> pa.Table:
    """sku_id → golden_sku_id, golden_mapping_status, unit_id.

    unavailable — связь ведёт в отсутствующий golden; conflict — цикл или несколько
    конечных golden; matched — ровно один конечный golden.
    """
    ids, terminal = resolve_terminals(golden)
    position = pc.fill_null(pc.index_in(links["golden_sku_id"], value_set=ids), -1)
    position = position.to_numpy(zero_copy_only=False)
    missing = position < 0
    resolved = np.where(missing, -1, terminal[np.where(missing, 0, position)])
    cyclic = ~missing & (resolved < 0)
    grouped = pa.table({
        "sku_id": links["sku_id"],
        "terminal": pa.array(resolved, mask=resolved < 0),
        "missing": pa.array(missing),
        "cyclic": pa.array(cyclic),
    }).group_by("sku_id").aggregate([
        ("terminal", "count_distinct"), ("terminal", "min"), ("missing", "max"), ("cyclic", "max"),
    ])
    status = pc.case_when(
        pc.make_struct(
            grouped["missing_max"],
            pc.or_(grouped["cyclic_max"], pc.greater(grouped["terminal_count_distinct"], 1)),
        ),
        "unavailable", "conflict", "matched",
    )
    matched = pc.equal(status, "matched")
    golden_id = pc.if_else(matched, pc.take(ids, pc.fill_null(grouped["terminal_min"], 0)),
                           pa.scalar(None, pa.string()))
    return pa.table({
        "sku_id": grouped["sku_id"],
        "golden_sku_id": golden_id,
        "golden_mapping_status": status,
    })
