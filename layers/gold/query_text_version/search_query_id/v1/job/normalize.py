"""Query normalization utils: stop-word cleanup and canonical query_id assembly."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

_WHITESPACE_PATTERN = re.compile(r"\s+")


def load_stop_words(path: str | Path) -> list[str]:
    """Read one stop word per line; blank lines and `#` comments are skipped."""
    words = []
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        words.append(line.lower())
    return words


def build_stop_words_pattern(words: Iterable[str]) -> re.Pattern | None:
    """Longest alternative first: `re` takes the leftmost match, so `aksiya` would otherwise
    shadow `aksiya tavarlar` and leave `tavarlar` in the query."""
    ordered_words = sorted({word for word in words if word}, key=lambda word: (-len(word), word))
    if not ordered_words:
        return None
    escaped_words = [re.escape(word) for word in ordered_words]
    return re.compile(r"\b(" + "|".join(escaped_words) + r")\b", flags=re.IGNORECASE)


def remove_stop_words(text: str, pattern: re.Pattern | None) -> str:
    """Lowercase, drop stop words, collapse whitespace. Empty pattern only normalizes spacing."""
    lowered = str(text).lower()
    if pattern is not None:
        lowered = pattern.sub(" ", lowered)
    return _WHITESPACE_PATTERN.sub(" ", lowered).strip()


def group_tokens_by_position(tokens: Sequence[Mapping[str, Any]]) -> list[list[str]]:
    """Group analyzer tokens by `position`; synonyms share a position and are deduplicated."""
    tokens_by_position: dict[Any, list[str]] = defaultdict(list)
    for token in tokens:
        tokens_by_position[token["position"]].append(token["token"])

    return [
        sorted(set(tokens_by_position[position]))
        for position in sorted(tokens_by_position)
    ]


def build_query_id(tokens: Sequence[Mapping[str, Any]]) -> str:
    """One canonical variant per position, sorted, so word order stops mattering."""
    canonical_tokens = [
        variants[0]
        for variants in group_tokens_by_position(tokens)
        if variants
    ]
    return " ".join(sorted(canonical_tokens))
