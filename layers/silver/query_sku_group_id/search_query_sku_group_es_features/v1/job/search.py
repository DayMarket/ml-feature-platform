"""Elasticsearch request builder for query/SKU group explain features."""

from __future__ import annotations

import time
from typing import Any, Mapping, Sequence

SOURCE_FIELDS = [
    "sku_group.id",
    "sku_group.price.sell",
    "sku_group.rating",
    "sku_group.orders_quantity",
    "product.id",
    "product.title.ru",
    "product.orders_quantity",
    "product.rating",
    "query_encoder_v3",
]

FIELD_VALUE_FACTORS = [
    "sku_group.rating",
    "sku_group.orders_quantity",
    "product.orders_quantity",
    "product.rating",
]

# The index stores one document per sku_id while the feature grain is sku_group_id.
# Collapse keeps one hit per group; the cardinality aggregation reports how many distinct
# groups matched, which is the only way to detect truncation by `size`.
COLLAPSE_FIELD = "sku_group.id"
TOTAL_AGGREGATION = "total"

# Same sort as the production query, so group ordering is reproducible between runs.
SORT_ORDER = [
    {"_score": {"order": "desc"}},
    {COLLAPSE_FIELD: {"order": "asc"}},
]


def _lexical_clauses(query: str, fields: Sequence[str]) -> list[dict[str, Any]]:
    """Build the lexical part of the query as one flat multi_match.

    Every configured field is addressed directly: the sku-level index has no nested
    documents, so no clause may be wrapped into `nested`. Fields carry no boost, and
    `most_fields` keeps the explain tree a plain sum of per-field scores, so each field
    contributes its own BM25 value to the output columns.

    `operator: or` is deliberate: production uses `and` with `minimum_should_match: 95%`,
    which would drop candidates that matched on only part of the query, and those
    candidates still need feature rows here.
    """
    if not fields:
        return []
    return [
        {
            "multi_match": {
                "query": query,
                "fields": list(fields),
                "type": "most_fields",
                "operator": "or",
            }
        }
    ]


def build_search_body(
    query: str,
    sku_group_ids: Sequence[int],
    fields: Sequence[str],
    size: int,
) -> dict[str, Any]:
    ids = [int(sku_group_id) for sku_group_id in sku_group_ids]
    functions = [
        {
            "field_value_factor": {
                "field": field,
                "factor": 1.0,
                "missing": 0,
            }
        }
        for field in FIELD_VALUE_FACTORS
    ]

    return {
        "size": int(size),
        "explain": True,
        "_source": SOURCE_FIELDS,
        "collapse": {"field": COLLAPSE_FIELD},
        "sort": SORT_ORDER,
        "aggregations": {
            TOTAL_AGGREGATION: {"cardinality": {"field": COLLAPSE_FIELD}},
        },
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "filter": [{"terms": {"sku_group.id": ids}}],
                        "should": _lexical_clauses(query, fields),
                        "minimum_should_match": 1,
                    }
                },
                "functions": functions,
                "score_mode": "sum",
                "boost_mode": "sum",
            }
        },
    }


def execute_search(
    url: str,
    body: Mapping[str, Any],
    auth: tuple[str, str] | None,
    headers: Mapping[str, str],
    timeout_seconds: int,
    retry_count: int,
):
    import requests

    last_error = None
    for attempt in range(1, retry_count + 1):
        try:
            response = requests.get(
                url=url,
                auth=auth,
                headers=dict(headers),
                json=dict(body),
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt < retry_count:
                time.sleep(min(attempt * 2, 10))
    raise RuntimeError(f"Elasticsearch request failed after {retry_count} attempts") from last_error
