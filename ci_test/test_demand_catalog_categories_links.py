"""Проверить нормализацию категорий и связей без скрытого dedup/fallback."""

from uuid import UUID

import pytest

from layers.silver.sku_id.demand_catalog_sku.v1.job.category_paths import RAW_LEVELS, build_category_index, missing_category
from layers.silver.sku_id.demand_catalog_sku.v1.job.golden_graph import resolve_golden_graph
from layers.silver.sku_id.demand_catalog_sku.v1.job.golden_links import prepare_active_links, resolve_sku_links
from ci_test.test_demand_catalog_golden_graph import row as golden_row


def category(key=10, levels=(1, 0, 0, 0, 0, 999)):
    return {"category_id": key, **dict(zip(RAW_LEVELS, levels, strict=True)), "l1_title": "L1", "leaf_title": "Leaf"}


def test_raw_levels_preserved_and_leaf_uses_actual_category():
    index, audit = build_category_index([category()], expected_rows=1)
    assert index[10]["raw_l2_category_id"] == 0 and index[10]["raw_l6_category_id"] == 999
    assert [index[10][f"l{n}"] for n in range(1, 6)] == [f"l{n}:1" for n in range(1, 6)]
    assert index[10]["leaf"] == "leaf:10" and audit["valid_category_rows"] == 1


def test_missing_l1_preserves_raw_without_making_unknown_node():
    index, audit = build_category_index([category(levels=(0, 2, 0, 0, 0, 0))], expected_rows=1)
    assert index[10]["category_path_status"] == "missing" and index[10]["raw_l2_category_id"] == 2
    assert index[10]["market"] is None and index[10]["leaf"] is None and audit["missing_category_rows"] == 1
    assert missing_category()["raw_l1_category_id"] is None


def test_conflicting_parent_marks_all_affected_categories_without_first_winner():
    rows = [category(10, (1, 3, 0, 0, 0, 0)), category(20, (2, 3, 0, 0, 0, 0)), category(30)]
    index, audit = build_category_index(rows, expected_rows=3)
    assert audit["conflicting_nodes"] == 1 and audit["conflict_category_rows"] == 2
    assert index[10]["category_path_status"] == index[20]["category_path_status"] == "conflict"
    assert index[10]["l2"] is None and index[10]["raw_l2_category_id"] == 3
    assert index[30]["category_path_status"] == "valid"
    assert build_category_index(reversed(rows), expected_rows=3) == (index, audit)


@pytest.mark.parametrize("field,value", [("category_id", 0), ("category_id", True),
    ("raw_l1_category_id", None), ("raw_l6_category_id", -1), ("raw_l2_category_id", 2**63), ("leaf_title", None)])
def test_bad_category_source_is_rejected(field, value):
    row = category()
    row[field] = value
    with pytest.raises(ValueError):
        build_category_index([row], expected_rows=1)


@pytest.mark.parametrize("rows,count", [([], 0), ([category()], 2), ([category(), category()], 2)])
def test_category_count_and_duplicate_checks(rows, count):
    with pytest.raises(ValueError):
        build_category_index(rows, expected_rows=count)


def link(sku=1, meta=100, golden=1):
    return dict(sku_id=sku, meta_sku_id=str(UUID(int=meta)), golden_sku_id=str(UUID(int=golden)))


def graph():
    return resolve_golden_graph([golden_row(1, 2), golden_row(2), golden_row(3)], expected_rows=3)[0]


def test_duplicates_and_converging_paths_do_not_create_a_false_conflict():
    rows = [link(), link(), link(golden=2)]
    result, audit = resolve_sku_links(rows, graph(), expected_rows=3)
    assert result[1]["golden_mapping_status"] == "matched"
    assert result[1]["unit_id"] == "g:" + str(UUID(int=2))
    assert audit["duplicate_relation_rows"] == 1 and audit["matched_sku"] == 1


def test_multiple_terminal_golden_is_conflict_not_arbitrary_matching():
    result, audit = resolve_sku_links([link(), link(golden=3)], graph(), expected_rows=2)
    assert result[1] == dict(golden_mapping_status="conflict", golden_sku_id=None, unit_id="s:1")
    assert audit["conflict_sku"] == 1


def test_unknown_golden_never_becomes_unmatched_even_with_known_candidate():
    for rows in ([link(golden=99)], [link(), link(golden=99)]):
        result, audit = resolve_sku_links(rows, graph(), expected_rows=len(rows))
        assert result[1] == dict(golden_mapping_status="unavailable", golden_sku_id=None, unit_id=None)
        assert audit["sku_with_missing_golden"] == 1 and 2 not in result


@pytest.mark.parametrize("field,value", [("sku_id", 0), ("sku_id", True), ("sku_id", 2**63),
    ("meta_sku_id", None), ("golden_sku_id", str(UUID(int=0)))])
def test_invalid_raw_link_is_not_discarded(field, value):
    row = link()
    row[field] = value
    with pytest.raises(ValueError):
        resolve_sku_links([row], graph(), expected_rows=1)


def test_link_capture_requires_full_count_and_closes_stream():
    for count in (0, 1, 3):
        closed = []
        def stream():
            try:
                yield link()
                yield link(sku=2)
            finally:
                closed.append(True)
        iterator = stream()
        with pytest.raises(ValueError):
            resolve_sku_links(iterator, graph(), expected_rows=count)
        if count:
            assert closed == [True]


def test_terminal_mapping_must_really_be_resolved():
    with pytest.raises(ValueError, match="terminal mapping"):
        resolve_sku_links([link()], {str(UUID(int=1)): str(UUID(int=2))}, expected_rows=1)


def raw_link(**changes):
    return {"meta_sku_id": str(UUID(int=100)), "golden_sku_id": str(UUID(int=1)), "link_provenance": "manual",
            "meta_present": 1, "meta_source": "uzum", "source_sku_id": "1", **changes}


def test_raw_links_filter_marketplace_not_link_provenance():
    records = [raw_link(), raw_link(link_provenance="review", source_sku_id="2"),
               raw_link(meta_source="other", source_sku_id="ASIN-text")]
    known, audit = prepare_active_links(records, expected_rows=3, expected_uzum_rows=2)
    assert [row["sku_id"] for row in known] == [1, 2]
    assert all(row["golden_sku_id"] == str(UUID(int=1)) for row in known)
    assert audit["orphan_meta_link_rows"] == 0


def test_orphan_is_skipped_and_audited_before_marketplace_filter():
    orphan = raw_link(meta_present=0, meta_source=None, source_sku_id=None)
    known, audit = prepare_active_links(
        [orphan, raw_link()],
        expected_rows=2,
        expected_uzum_rows=1,
    )
    assert [row["sku_id"] for row in known] == [1]
    assert audit == {
        "source_active_link_rows": 2,
        "orphan_meta_link_rows": 1,
        "recognized_uzum_link_rows": 1,
    }


@pytest.mark.parametrize("changes", [{"meta_present": True}, {"meta_source": ""}, {"source_sku_id": "not-id"},
    {"source_sku_id": "0"}, {"source_sku_id": str(2**63)}])
def test_invalid_identity_never_becomes_unmatched(changes):
    with pytest.raises(ValueError):
        prepare_active_links([raw_link(**changes)], expected_rows=1, expected_uzum_rows=1)


def test_raw_link_counts_and_cursor_closure():
    closed = []
    def stream():
        try:
            yield raw_link()
        finally:
            closed.append(True)
    with pytest.raises(ValueError, match="Неполный"):
        prepare_active_links(stream(), expected_rows=2, expected_uzum_rows=1)
    assert closed == [True]
