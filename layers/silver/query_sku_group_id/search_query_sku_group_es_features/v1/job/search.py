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


def _field_path(field: str) -> str | None:
    if field.startswith("skus.discovery_filter_values."):
        return "skus.discovery_filter_values"
    if field.startswith("skus.filter_values."):
        return "skus.filter_values"
    if field.startswith("skus."):
        return "skus"
    return None


def _lexical_clauses(query: str, fields: Sequence[str]) -> list[dict[str, Any]]:
    grouped: dict[str | None, list[str]] = {}
    for field in fields:
        grouped.setdefault(_field_path(field), []).append(field)

    clauses = []
    root_fields = grouped.get(None, [])
    if root_fields:
        clauses.append(
            {
                "multi_match": {
                    "query": query,
                    "fields": root_fields,
                    "operator": "or",
                }
            }
        )

    for path, path_fields in sorted(grouped.items(), key=lambda item: str(item[0])):
        if path is None:
            continue
        clauses.append(
            {
                "nested": {
                    "path": path,
                    "score_mode": "sum",
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": path_fields,
                            "operator": "or",
                        }
                    },
                }
            }
        )
    return clauses


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
