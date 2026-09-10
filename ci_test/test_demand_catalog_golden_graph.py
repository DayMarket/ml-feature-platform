"""Проверить полный MDM-граф, циклы, missing targets и нерекурсивные длинные цепочки."""

from uuid import UUID

import pytest

from layers.silver.sku_id.demand_catalog_sku.v1.job.golden_graph import resolve_golden_graph


def row(node, target=0, flag=None):
    return dict(golden_sku_id=str(UUID(int=node)), merged_into=str(UUID(int=target)),
                is_merged=int(target != 0) if flag is None else flag)


def test_chain_branch_and_terminal_are_order_independent():
    records = [row(1, 2), row(2, 3), row(3), row(4, 2), row(5)]
    resolved, audit = resolve_golden_graph(records, expected_rows=5)
    assert resolved == {str(UUID(int=node)): str(UUID(int=3 if node < 5 else 5)) for node in range(1, 6)}
    assert audit == dict(golden_rows=5, merged_rows=3, terminal_rows=2, multi_hop_rows=2, max_merge_hops=2)
    assert resolve_golden_graph(reversed(records), expected_rows=5) == (resolved, audit)


def test_long_chain_does_not_depend_on_python_recursion_limit():
    count = 5000
    resolved, audit = resolve_golden_graph((row(n, n + 1 if n < count else 0) for n in range(1, count + 1)),
                                          expected_rows=count)
    assert set(resolved.values()) == {str(UUID(int=count))}
    assert audit["max_merge_hops"] == count - 1


@pytest.mark.parametrize("records", [[row(1, 1)], [row(1, 2), row(2, 1)],
    [row(1), row(2, 3), row(3, 4), row(4, 2)], [row(1, 2)], [row(1), row(1)]])
def test_cycles_missing_targets_and_duplicates_block_whole_graph(records):
    with pytest.raises(ValueError, match="Цикл|отсутствует|Повтор"):
        resolve_golden_graph(records, expected_rows=len(records))


@pytest.mark.parametrize("field,value", [("golden_sku_id", None), ("golden_sku_id", "wrong"),
    ("golden_sku_id", str(UUID(int=0))), ("is_merged", True), ("is_merged", 2),
    ("is_merged", "1"), ("merged_into", None)])
def test_bad_raw_contract_rejected(field, value):
    record = row(1)
    record[field] = value
    with pytest.raises(ValueError):
        resolve_golden_graph([record], expected_rows=1)


def test_zero_target_of_merged_node_rejected_and_terminal_flag_is_authoritative():
    with pytest.raises(ValueError, match="Нулевой"):
        resolve_golden_graph([row(1, flag=1)], expected_rows=1)
    resolved, _ = resolve_golden_graph([row(1, 99, flag=0)], expected_rows=1)
    assert resolved == {str(UUID(int=1)): str(UUID(int=1))}


@pytest.mark.parametrize("count", [None, True, 0, 2])
def test_incomplete_count_and_bad_metadata_rejected(count):
    with pytest.raises(ValueError):
        resolve_golden_graph([row(1)], expected_rows=count)


def test_stream_closed_on_invalid_row_and_too_many_rows():
    for records in ([row(1), row(1)], [row(1), row(2)]):
        closed = []
        def stream():
            try:
                yield from records
            finally:
                closed.append(True)
        with pytest.raises(ValueError):
            resolve_golden_graph(stream(), expected_rows=1)
        assert closed == [True]
