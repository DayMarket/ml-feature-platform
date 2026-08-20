"""Чтение и валидация блока dq: из config.yaml энтити."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any


class DqConfigError(ValueError):
    """Некорректный блок dq: в config.yaml."""


SEVERITIES = ("error", "warn")

# Семейства совпадают с таксономией dbt-trino/scripts/check_model_tests_score.py,
# чтобы у нас и у DE был общий словарь при разговоре о покрытии.
TEST_FAMILIES: dict[str, str] = {
    "primary_key_not_null": "null_checks",
    "primary_key_unique": "uniqueness",
    "row_count_min": "consistency",
    "row_count_growth": "consistency",
    "freshness": "recency",
    "not_null": "null_checks",
    "null_share_below": "null_checks",
    "unique_combination": "uniqueness",
    "accepted_values": "domain_values",
    "not_accepted_values": "domain_values",
    "accepted_range": "domain_values",
    "non_negative": "domain_values",
    "string_not_blank": "domain_values",
    "distinct_count_between": "consistency",
    "columns_sum_equals": "consistency",
    "row_count_matches_reference": "consistency",
    "expression_is_true": "row_expr",
    "relationships": "referential_integrity",
}

# name -> (обязательные параметры, необязательные параметры с дефолтами)
TEST_PARAMS: dict[str, tuple[tuple[str, ...], dict[str, Any]]] = {
    "primary_key_not_null": ((), {}),
    "primary_key_unique": ((), {}),
    "row_count_min": ((), {"min_rows": 0}),
    "row_count_growth": ((), {"max_growth_ratio": 0.2, "direction": "both"}),
    "freshness": ((), {"max_lag_days": 2}),
    "not_null": (("columns",), {"max_null_share": None}),
    "null_share_below": (("column", "max_share"), {}),
    "unique_combination": (("columns",), {}),
    "accepted_values": (("column", "values"), {"ignore_nulls": True}),
    "not_accepted_values": (("column", "values"), {"ignore_nulls": True}),
    "accepted_range": (
        ("column",),
        {"min": None, "max": None, "min_inclusive": True, "max_inclusive": True, "ignore_nulls": True},
    ),
    "non_negative": (("columns",), {"ignore_nulls": True}),
    "string_not_blank": (("columns",), {}),
    "distinct_count_between": (("columns",), {"min": None, "max": None}),
    "columns_sum_equals": (("parts", "total"), {"tolerance": 1e-6}),
    "row_count_matches_reference": (
        ("reference_table", "reference_date_column"),
        {"reference_where": None, "tolerance_ratio": 0.0},
    ),
    "expression_is_true": (("expression",), {}),
    "relationships": (("column", "to_table", "to_column"), {"where": None}),
}

BASE_TESTS = (
    "primary_key_not_null",
    "primary_key_unique",
    "row_count_min",
    "row_count_growth",
    "freshness",
)

DIRECTIONS = ("both", "up", "down")


@dataclass(frozen=True)
class TestSpec:
    name: str
    family: str
    params: dict[str, Any]
    severity: str = "error"
    where: str | None = None


@dataclass(frozen=True)
class DqSettings:
    enabled: bool
    trino_conn_id: str
    scope: str
    partition_column: str
    partition_date_template: str
    sample_rows: int
    query_timeout_seconds: int
    warmup_days: int
    active_from: date | None
    tests: tuple[TestSpec, ...] = field(default=())


@dataclass(frozen=True)
class RenderContext:
    catalog_alias: str
    schema: str
    table: str
    primary_key: tuple[str, ...]
    partition_column: str
    partition_date: date
    scope: str
    sample_rows: int


DEFAULT_PARTITION_DATE_TEMPLATE = '{{ macros.ds_add(ds, -1) }}'


def load_dq_settings(config: dict[str, Any]) -> DqSettings:
    table = config.get("table") or {}
    primary_key = _parse_primary_key(table.get("primary_key", ""))
    if not primary_key:
        raise DqConfigError("table.primary_key обязателен для DQ")

    raw = config.get("dq") or {}
    if not isinstance(raw, dict):
        raise DqConfigError("Блок dq: должен быть отображением")

    scope = str(raw.get("scope", "partition"))
    if scope not in ("partition", "table"):
        raise DqConfigError(f"dq.scope должен быть partition или table, получено {scope!r}")

    warmup_days = int(raw.get("warmup_days", 1))
    if warmup_days < 0:
        raise DqConfigError("dq.warmup_days не может быть отрицательным")

    active_from_raw = raw.get("active_from")
    active_from = date.fromisoformat(str(active_from_raw)) if active_from_raw else None

    return DqSettings(
        enabled=_parse_bool(raw.get("enabled", True), "dq.enabled"),
        trino_conn_id=str(raw.get("trino_conn_id", "trino_search")),
        scope=scope,
        partition_column=str(raw.get("partition_column", "date")),
        partition_date_template=str(raw.get("partition_date_template", DEFAULT_PARTITION_DATE_TEMPLATE)),
        sample_rows=int(raw.get("sample_rows", 5)),
        query_timeout_seconds=int(raw.get("query_timeout_seconds", 600)),
        warmup_days=warmup_days,
        active_from=active_from,
        tests=_build_specs(raw.get("tests") or []),
    )


def _build_specs(raw_tests: Any) -> tuple[TestSpec, ...]:
    if not isinstance(raw_tests, list):
        raise DqConfigError("dq.tests должен быть списком")

    overrides: dict[str, dict[str, Any]] = {}
    extras: list[dict[str, Any]] = []
    for entry in raw_tests:
        if not isinstance(entry, dict) or "name" not in entry:
            raise DqConfigError(
                f"Каждый элемент dq.tests должен быть отображением с ключом name: {entry!r}"
            )
        name = str(entry["name"])
        if name not in TEST_PARAMS:
            raise DqConfigError(
                f"Неизвестный DQ-тест {name!r}. Доступные: {', '.join(sorted(TEST_PARAMS))}"
            )
        if name in BASE_TESTS:
            if name in overrides:
                raise DqConfigError(f"Базовый тест {name!r} переопределён дважды")
            overrides[name] = entry
        else:
            extras.append(entry)

    specs: list[TestSpec] = []
    for name in BASE_TESTS:
        entry = overrides.get(name, {"name": name})
        if not _parse_bool(entry.get("enabled", True), f"dq.tests[{name}].enabled"):
            continue
        specs.append(_make_spec(entry))
    for entry in extras:
        if not _parse_bool(entry.get("enabled", True), f"dq.tests[{entry['name']}].enabled"):
            continue
        specs.append(_make_spec(entry))
    return tuple(specs)


def _make_spec(entry: dict[str, Any]) -> TestSpec:
    name = str(entry["name"])
    required, optional = TEST_PARAMS[name]

    severity = str(entry.get("severity", "error"))
    if severity not in SEVERITIES:
        raise DqConfigError(
            f"{name}: severity должен быть одним из {SEVERITIES}, получено {severity!r}"
        )

    where = entry.get("where")
    if name == "relationships":
        where = None  # у relationships where — параметр теста, а не общий фильтр

    reserved = {"name", "severity", "enabled"} | ({"where"} if name != "relationships" else set())
    unknown = set(entry) - reserved - set(required) - set(optional)
    if unknown:
        raise DqConfigError(f"{name}: неизвестные параметры {sorted(unknown)}")

    params: dict[str, Any] = {}
    for key in required:
        if key not in entry:
            raise DqConfigError(f"{name}: обязательный параметр {key!r} не задан")
        params[key] = entry[key]
    for key, default in optional.items():
        if key in entry:
            params[key] = entry[key]
        elif default is not None or name in BASE_TESTS:
            params[key] = default

    if name == "row_count_growth" and params.get("direction") not in DIRECTIONS:
        raise DqConfigError(f"row_count_growth: direction должен быть одним из {DIRECTIONS}")
    if name == "accepted_range" and params.get("min") is None and params.get("max") is None:
        raise DqConfigError("accepted_range: нужен хотя бы один из параметров min/max")
    if name == "distinct_count_between" and params.get("min") is None and params.get("max") is None:
        raise DqConfigError("distinct_count_between: нужен хотя бы один из параметров min/max")

    return TestSpec(
        name=name,
        family=TEST_FAMILIES[name],
        params=params,
        severity=severity,
        where=str(where) if where else None,
    )


def _parse_primary_key(primary_key: str) -> tuple[str, ...]:
    return tuple(column.strip() for column in str(primary_key).split(",") if column.strip())


def _parse_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().strip('"').strip("'").lower()
    if normalized in {"true", "yes", "1"}:
        return True
    if normalized in {"false", "no", "0"}:
        return False
    raise DqConfigError(f"{field_name} должен быть булевым, получено {value!r}")


def trino_catalog_alias(repo_root: Path, catalog: str) -> str:
    """Маппинг каталога config.yaml в имя каталога Trino через ci_config.yaml."""
    ci_config = json.loads((Path(repo_root) / "ci_config.yaml").read_text(encoding="utf-8"))
    mapping = ci_config["dbt"]["database_mapping"]
    if catalog not in mapping:
        raise DqConfigError(f"В ci_config.yaml нет маппинга для каталога {catalog!r}")
    return str(mapping[catalog])
