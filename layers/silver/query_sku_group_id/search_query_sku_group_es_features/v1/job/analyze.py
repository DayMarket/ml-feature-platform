"""Parse Elasticsearch explain payloads into feature rows."""

from __future__ import annotations

import json
import math
import re
from typing import Any, Mapping, Sequence

CORE_COLUMNS = [
    "date",
    "query",
    "sku_group_id",
    "product_id",
    "sku_group_title",
    "sell_price",
    "skg_rating_field",
    "skg_orders_field",
    "product_orders_field",
    "product_rating_field",
    "skg_orders",
    "product_orders",
    "skg_rating",
    "product_rating",
    "bms",
    "total_score",
    "sku_group_emb",
    "analysis",
]


def bm25_column(field: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", field).strip("_").lower()
    return f"bm25_{normalized}"


def bm25_columns(fields: Sequence[str]) -> list[str]:
    return [bm25_column(field) for field in fields]


def output_columns(fields: Sequence[str]) -> list[str]:
    return CORE_COLUMNS[:-1] + bm25_columns(fields) + CORE_COLUMNS[-1:]


def calculate_bm25(
    boost: float,
    n: int,
    N: int,
    freq: float,
    dl: float,
    avgdl: float,
    b: float = 0.5,
    k1: float = 1.2,
) -> float:
    idf = math.log(1 + (N - n + 0.5) / (n + 0.5))
    tf = freq / (freq + k1 * (1 - b + b * (dl / avgdl)))
    return boost * idf * tf


def _float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _list_float(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        try:
            result.append(float(item))
        except (TypeError, ValueError):
            continue
    return result


def _extract_bm25_params(node: Mapping[str, Any]) -> dict[str, Any]:
    params = {
        "boost": None,
        "idf_n": None,
        "idf_N": None,
        "tf_freq": None,
        "tf_dl": None,
        "tf_avgdl": None,
        "tf_b": 0.5,
        "tf_k1": 1.2,
    }

    for detail in node.get("details", []):
        if not isinstance(detail, Mapping):
            continue
        description = str(detail.get("description", ""))
        value = detail.get("value")

        if description == "boost":
            params["boost"] = value
        elif "idf, computed as" in description:
            for sub in detail.get("details", []):
                if not isinstance(sub, Mapping):
                    continue
                sub_description = str(sub.get("description", ""))
                if "n, number of documents" in sub_description:
                    params["idf_n"] = sub.get("value")
                elif "N, total number of documents" in sub_description:
                    params["idf_N"] = sub.get("value")
        elif "tf, computed as" in description:
            for sub in detail.get("details", []):
                if not isinstance(sub, Mapping):
                    continue
                sub_description = str(sub.get("description", ""))
                if "freq" in sub_description or "termFreq=" in sub_description:
                    params["tf_freq"] = sub.get("value")
                elif "k1" in sub_description:
                    params["tf_k1"] = sub.get("value")
                elif "b, length normalization" in sub_description:
                    params["tf_b"] = sub.get("value")
                elif "dl, length of field" in sub_description:
                    params["tf_dl"] = sub.get("value")
                elif "avgdl" in sub_description:
                    params["tf_avgdl"] = sub.get("value")

    return params


def _field_and_base_term(description: str) -> tuple[str, str, list[str]]:
    if "Synonym(" in description:
        start = description.find("Synonym(") + len("Synonym(")
        end = description.find(") in", start)
        if end == -1:
            end = description.find(")", start)

        if start < end:
            content = description[start:end]
            field = "unknown"
            terms = []
            for item in content.split():
                if ".synonym:" in item:
                    field_part, term = item.split(".synonym:", 1)
                    field = f"{field_part}.synonym"
                    terms.append(term)
                elif ":" in item:
                    field_part, term = item.split(":", 1)
                    field = field_part
                    terms.append(term)
            if terms:
                return field, terms[0], terms

    match = re.search(r"weight\(([^:]+):([^ )]+) in", description)
    if match:
        field, term = match.group(1), match.group(2)
        return field, term, [term]

    return "unknown", "unknown", []


def extract_bm25_simple_grouped(explanation: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    def traverse(node: Any, parent_description: str | None = None) -> None:
        if not isinstance(node, Mapping):
            return

        description = str(node.get("description", ""))
        value = node.get("value")

        if "score(freq" in description and value is not None and parent_description:
            if "weight(" in parent_description or "Synonym(" in parent_description:
                field, base_term, terms = _field_and_base_term(parent_description)
                if field != "unknown" and base_term != "unknown":
                    params = _extract_bm25_params(node)
                    calculated = None
                    required = [
                        params["boost"],
                        params["idf_n"],
                        params["idf_N"],
                        params["tf_freq"],
                        params["tf_dl"],
                        params["tf_avgdl"],
                    ]
                    if all(item is not None for item in required):
                        try:
                            calculated = calculate_bm25(
                                boost=float(params["boost"]),
                                n=int(params["idf_n"]),
                                N=int(params["idf_N"]),
                                freq=float(params["tf_freq"]),
                                dl=float(params["tf_dl"]),
                                avgdl=float(params["tf_avgdl"]),
                                b=float(params["tf_b"] if params["tf_b"] is not None else 0.5),
                                k1=float(params["tf_k1"] if params["tf_k1"] is not None else 1.2),
                            )
                        except (TypeError, ValueError, ZeroDivisionError):
                            calculated = None

                    key = f"{field}:{base_term}"
                    if key in result:
                        result[key]["original_score"] += _float(value)
                        result[key]["all_terms"] = sorted(
                            set(result[key]["all_terms"]).union(terms)
                        )
                    else:
                        result[key] = {
                            "field": field,
                            "term": base_term,
                            "all_terms": terms,
                            "boost": params["boost"],
                            "idf": {"n": params["idf_n"], "N": params["idf_N"]},
                            "tf": {
                                "freq": params["tf_freq"],
                                "dl": params["tf_dl"],
                                "avgdl": params["tf_avgdl"],
                                "b": params["tf_b"] if params["tf_b"] is not None else 0.5,
                                "k1": params["tf_k1"] if params["tf_k1"] is not None else 1.2,
                            },
                            "original_score": _float(value),
                            "calculated_score": calculated,
                            "is_synonym": "Synonym(" in parent_description,
                            "description": parent_description[:160],
                        }

        for detail in node.get("details", []):
            traverse(detail, description)

    traverse(explanation)
    return result


def extract_field_factors(explanation: Mapping[str, Any]) -> dict[str, Any]:
    result = {"field_factors": {}}

    def traverse(node: Any) -> None:
        if isinstance(node, Mapping):
            description = str(node.get("description", ""))
            value = _float(node.get("value"))

            if "field value function:" in description:
                field_match = re.search(r"doc\['([^']+)'\]", description)
                if field_match:
                    modifier = "none"
                    if "log1p" in description:
                        modifier = "log1p"
                    elif "sqrt" in description:
                        modifier = "sqrt"
                    elif "square" in description:
                        modifier = "square"
                    elif "log" in description:
                        modifier = "log"

                    result["field_factors"][field_match.group(1)] = {
                        "value": value,
                        "modifier": modifier,
                        "description": description,
                    }

            for detail in node.get("details", []):
                traverse(detail)
        elif isinstance(node, list):
            for item in node:
                traverse(item)

    traverse(explanation)
    return result


def analyze_explain(explain_data: Mapping[str, Any]) -> dict[str, Any]:
    simple_grouping = extract_bm25_simple_grouped(explain_data)
    field_factors = extract_field_factors(explain_data)
    return {
        "total_score": _float(explain_data.get("value")),
        "field_factors": field_factors["field_factors"],
        "simple_grouping": {
            "total_bm25": sum(
                item["original_score"] for item in simple_grouping.values()
            ),
            "scores": simple_grouping,
        },
    }


def hit_to_row(
    hit: Mapping[str, Any],
    query: str,
    partition_date,
    fields: Sequence[str],
) -> dict[str, Any]:
    source = hit.get("_source", {})
    if not isinstance(source, Mapping):
        source = {}

    sku_group = source.get("sku_group", {})
    if not isinstance(sku_group, Mapping):
        sku_group = {}

    product = source.get("product", {})
    if not isinstance(product, Mapping):
        product = {}

    title = product.get("title", {})
    if not isinstance(title, Mapping):
        title = {}

    price = sku_group.get("price", {})
    if not isinstance(price, Mapping):
        price = {}

    analysis = analyze_explain(hit.get("_explanation", {}))
    field_factors = analysis.get("field_factors", {})

    row = {
        "date": partition_date,
        "query": query,
        "sku_group_id": _int(sku_group.get("id")),
        "product_id": _int(product.get("id")),
        "sku_group_title": str(title.get("ru", "") or ""),
        "sell_price": _float(price.get("sell")),
        "skg_rating_field": _float(
            field_factors.get("sku_group.rating", {}).get("value")
        ),
        "skg_orders_field": _float(
            field_factors.get("sku_group.orders_quantity", {}).get("value")
        ),
        "product_orders_field": _float(
            field_factors.get("product.orders_quantity", {}).get("value")
        ),
        "product_rating_field": _float(
            field_factors.get("product.rating", {}).get("value")
        ),
        "skg_orders": _float(sku_group.get("orders_quantity")),
        "product_orders": _float(product.get("orders_quantity")),
        "skg_rating": _float(sku_group.get("rating")),
        "product_rating": _float(product.get("rating")),
        "bms": _float(analysis.get("simple_grouping", {}).get("total_bm25")),
        "total_score": _float(analysis.get("total_score")),
        "sku_group_emb": _list_float(source.get("query_encoder_v3", [])),
    }

    grouped_scores = analysis.get("simple_grouping", {}).get("scores", {})
    for field in fields:
        column = bm25_column(field)
        row[column] = [
            _float(item.get("original_score"))
            for item in grouped_scores.values()
            if item.get("field") == field
        ]

    row["analysis"] = json.dumps(analysis, ensure_ascii=False, sort_keys=True)
    return row
