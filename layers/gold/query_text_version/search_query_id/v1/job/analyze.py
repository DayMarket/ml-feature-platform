"""Elasticsearch `_analyze` client returning analyzer tokens for one query."""

from __future__ import annotations

import time
from typing import Any, Mapping


def build_analyze_body(analyzer: str, text: str) -> dict[str, Any]:
    return {"analyzer": analyzer, "text": str(text)}


def analyze_query_tokens(
    url: str,
    analyzer: str,
    query: str,
    auth: tuple[str, str] | None,
    headers: Mapping[str, str],
    timeout_seconds: int,
    retry_count: int,
) -> list[dict[str, Any]]:
    import requests

    if retry_count < 1:
        raise ValueError("retry_count must be at least 1")

    body = build_analyze_body(analyzer, query)
    last_error = None
    for attempt in range(1, retry_count + 1):
        try:
            response = requests.get(
                url=url,
                auth=auth,
                headers=dict(headers),
                json=body,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return response.json().get("tokens", [])
        except requests.RequestException as exc:
            last_error = exc
            if attempt < retry_count:
                time.sleep(min(attempt * 2, 10))

    raise RuntimeError(
        f"Elasticsearch analyzer failed after {retry_count} attempts for query {query!r}"
    ) from last_error
