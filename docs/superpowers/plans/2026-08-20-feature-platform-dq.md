# Feature Platform DQ — Implementation Plan (фазы 0–1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** DQ-тесты объявляются в `config.yaml` энтити и исполняются таской `dq` внутри того же DAG'а сразу после записи партиции, с подробным отчётом в логе, oncall-алертом и историей в Iceberg.

**Architecture:** Новый top-level пакет `dq/` — чистые функции рендера SQL (`tests.py`), исполнитель поверх `TrinoHook` (`runner.py`), форматтер отчёта (`report.py`), запись истории через `pyiceberg` (`results.py`) и фабрика Airflow-таски (`task.py`). Конфиг энтити читается `yaml.safe_load`, таблицы адресуются в Trino как `"dwh-iceberg".<schema>.<table>`. Каждый тест рендерится в SQL, возвращающий ровно одну строку `(failed_rows BIGINT, observed DOUBLE)`; `failed_rows > 0` — падение, `failed_rows < 0` — `skipped`.

**Tech Stack:** Python 3.11, Airflow 3 (`airflow.sdk`), `airflow.providers.trino.hooks.trino.TrinoHook`, `pyiceberg`, `PyYAML`, Trino/Iceberg, Drone CI.

**Spec:** `docs/superpowers/specs/2026-08-20-feature-platform-dq-design.md`

## Global Constraints

- Ветка работы: `feat/feature-platform-dq`. В `master` не коммитить.
- `AGENTS.md` — обязателен к соблюдению. Общий код только в новом top-level `dq/`, никаких `layers/_common`.
- `config.yaml` — единственный источник правды для идентификаторов таблиц. Никаких констант `"schema.table"` в коде.
- PyIceberg-идентификатор — всегда двухэлементный кортеж `(schema, name)`, построенный напрямую из конфига. Никаких `split`/`rsplit`.
- Миграции: `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, обязательный `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')`. Никаких `DROP`/`DELETE`/`TRUNCATE`.
- Тесты в `ci_test/` пишутся в существующем стиле репозитория: обычный python-скрипт с `def main() -> int`, голыми `assert` и `raise SystemExit(main())`. **Не pytest** — Drone их не запускает, они входят в «Local Validation Commands».
- CI-гейты живут в `scripts/` и вызываются из `.drone.yaml` (образец — `scripts/validate_ranking_upload_configs.py`).
- Все команды запускаются из корня репозитория.
- Маппинг каталога `iceberg` → `dwh-iceberg` берётся из `ci_config.yaml` (`dbt.database_mapping`), не хардкодится.
- Никаких обращений к проду из тестов. Всё, что в `ci_test/`, работает офлайн.

---

### Task 1: Чтение и валидация блока `dq:`

**Files:**
- Create: `dq/__init__.py`
- Create: `dq/config.py`
- Test: `ci_test/test_dq_config.py`

**Interfaces:**
- Consumes: ничего.
- Produces: `dq.config.TestSpec`, `dq.config.DqSettings`, `dq.config.RenderContext`, `dq.config.load_dq_settings(config: dict) -> DqSettings`, `dq.config.trino_catalog_alias(repo_root: Path, catalog: str) -> str`, `dq.config.DqConfigError`.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_dq_config.py`:

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import DqConfigError, load_dq_settings, trino_catalog_alias


def test_defaults_without_dq_block() -> None:
    settings = load_dq_settings(
        {"table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"}}
    )
    assert settings.enabled is True
    assert settings.trino_conn_id == "trino_search"
    assert settings.scope == "partition"
    assert settings.partition_column == "date"
    assert settings.sample_rows == 5
    assert settings.query_timeout_seconds == 600
    assert settings.warmup_days == 1
    assert settings.active_from is None
    assert [spec.name for spec in settings.tests] == [
        "primary_key_not_null",
        "primary_key_unique",
        "row_count_min",
        "row_count_growth",
        "freshness",
    ]
    by_name = {spec.name: spec for spec in settings.tests}
    assert by_name["row_count_min"].params["min_rows"] == 0
    assert by_name["row_count_growth"].params["max_growth_ratio"] == 0.2
    assert by_name["row_count_growth"].params["direction"] == "both"
    assert by_name["freshness"].params["max_lag_days"] == 2
    assert all(spec.severity == "error" for spec in settings.tests)


def test_base_test_override_and_disable() -> None:
    settings = load_dq_settings(
        {
            "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
            "dq": {
                "tests": [
                    {"name": "row_count_min", "min_rows": 1000},
                    {"name": "row_count_growth", "enabled": False},
                ]
            },
        }
    )
    by_name = {spec.name: spec for spec in settings.tests}
    assert by_name["row_count_min"].params["min_rows"] == 1000
    assert "row_count_growth" not in by_name


def test_optional_test_severity_and_where() -> None:
    settings = load_dq_settings(
        {
            "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
            "dq": {
                "tests": [
                    {
                        "name": "accepted_range",
                        "column": "conversion_rate",
                        "min": 0,
                        "max": 1,
                        "severity": "warn",
                        "where": "platform = 'ios'",
                    }
                ]
            },
        }
    )
    spec = [item for item in settings.tests if item.name == "accepted_range"][0]
    assert spec.severity == "warn"
    assert spec.where == "platform = 'ios'"
    assert spec.family == "domain_values"
    assert spec.params == {"column": "conversion_rate", "min": 0, "max": 1}


def test_unknown_test_name_rejected() -> None:
    try:
        load_dq_settings(
            {
                "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
                "dq": {"tests": [{"name": "no_such_test"}]},
            }
        )
    except DqConfigError as error:
        assert "no_such_test" in str(error)
    else:
        raise AssertionError("unknown test name must be rejected")


def test_missing_required_param_rejected() -> None:
    try:
        load_dq_settings(
            {
                "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
                "dq": {"tests": [{"name": "accepted_values", "column": "platform"}]},
            }
        )
    except DqConfigError as error:
        assert "values" in str(error)
    else:
        raise AssertionError("missing required param must be rejected")


def test_bad_severity_rejected() -> None:
    try:
        load_dq_settings(
            {
                "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
                "dq": {"tests": [{"name": "non_negative", "columns": ["x"], "severity": "critical"}]},
            }
        )
    except DqConfigError as error:
        assert "severity" in str(error)
    else:
        raise AssertionError("bad severity must be rejected")


def test_catalog_alias_from_ci_config() -> None:
    assert trino_catalog_alias(Path("."), "iceberg") == "dwh-iceberg"


def main() -> int:
    test_defaults_without_dq_block()
    test_base_test_override_and_disable()
    test_optional_test_severity_and_where()
    test_unknown_test_name_rejected()
    test_missing_required_param_rejected()
    test_bad_severity_rejected()
    test_catalog_alias_from_ci_config()
    print("DQ config tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_config.py`
Expected: FAIL с `ModuleNotFoundError: No module named 'dq'`

- [ ] **Step 3: Реализовать `dq/config.py`**

Создать пустой `dq/__init__.py` и `dq/config.py`:

```python
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
    "accepted_range": (("column",), {"min": None, "max": None, "min_inclusive": True, "max_inclusive": True, "ignore_nulls": True}),
    "non_negative": (("columns",), {"ignore_nulls": True}),
    "string_not_blank": (("columns",), {}),
    "distinct_count_between": (("columns",), {"min": None, "max": None}),
    "columns_sum_equals": (("parts", "total"), {"tolerance": 1e-6}),
    "row_count_matches_reference": (("reference_table", "reference_date_column"), {"reference_where": None, "tolerance_ratio": 0.0}),
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
            raise DqConfigError(f"Каждый элемент dq.tests должен быть отображением с ключом name: {entry!r}")
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
        raise DqConfigError(f"{name}: severity должен быть одним из {SEVERITIES}, получено {severity!r}")

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
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_dq_config.py`
Expected: PASS, вывод `DQ config tests completed successfully`

- [ ] **Step 5: Коммит**

```bash
git add dq/__init__.py dq/config.py ci_test/test_dq_config.py
git commit -m "feat(dq): config reader and validation for dq blocks"
```

---

### Task 2: Рендер SQL — базовые пять тестов

**Files:**
- Create: `dq/tests.py`
- Test: `ci_test/test_dq_sql.py`

**Interfaces:**
- Consumes: `dq.config.TestSpec`, `dq.config.RenderContext`.
- Produces: `dq.tests.RenderedTest` (поля `spec, test_key, sql, sample_sql, threshold, needs_baseline`), `dq.tests.render(spec: TestSpec, ctx: RenderContext) -> RenderedTest`, `dq.tests.quote_identifier(name: str) -> str`, `dq.tests.quote_literal(value) -> str`, `dq.tests.table_ref(ctx) -> str`, `dq.tests.scope_predicate(ctx) -> str`.

Контракт SQL: каждый запрос возвращает **ровно одну строку** с колонками `failed_rows BIGINT` и `observed DOUBLE`. `failed_rows > 0` — падение, `failed_rows = 0` — успех, `failed_rows < 0` — тест не мог быть выполнен (пропуск).

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_dq_sql.py`:

```python
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext, TestSpec
from dq.tests import quote_literal, render

CTX = RenderContext(
    catalog_alias="dwh-iceberg",
    schema="silver",
    table="feature_platform_sku_group_id_prices",
    primary_key=("date", "sku_group_id"),
    partition_column="date",
    partition_date=date(2026, 8, 19),
    scope="partition",
    sample_rows=5,
)

TABLE = '"dwh-iceberg"."silver"."feature_platform_sku_group_id_prices"'


def spec(name: str, **params) -> TestSpec:
    from dq.config import TEST_FAMILIES

    severity = params.pop("severity", "error")
    where = params.pop("where", None)
    return TestSpec(name=name, family=TEST_FAMILIES[name], params=params, severity=severity, where=where)


def test_primary_key_not_null() -> None:
    rendered = render(spec("primary_key_not_null"), CTX)
    assert rendered.test_key == "primary_key_not_null"
    assert TABLE in rendered.sql
    assert '"date" IS NULL OR "sku_group_id" IS NULL' in rendered.sql
    assert '"date" = DATE \'2026-08-19\'' in rendered.sql
    assert rendered.sample_sql is not None
    assert "LIMIT 5" in rendered.sample_sql
    assert rendered.needs_baseline is False


def test_primary_key_unique() -> None:
    rendered = render(spec("primary_key_unique"), CTX)
    assert 'GROUP BY "date", "sku_group_id"' in rendered.sql
    assert "HAVING count(*) > 1" in rendered.sql


def test_row_count_min_uses_non_strict_comparison() -> None:
    rendered = render(spec("row_count_min", min_rows=0), CTX)
    # Дословная семантика dbt-макроса row_count_greater_than_for_date: падение при row_count <= min_rows.
    assert "WHEN row_count <= 0 THEN 1" in rendered.sql
    assert rendered.sample_sql is None
    assert rendered.threshold == "row_count > 0"


def test_row_count_growth_two_sided_and_skips_without_baseline() -> None:
    rendered = render(spec("row_count_growth", max_growth_ratio=0.2, direction="both"), CTX)
    assert rendered.needs_baseline is True
    assert "WHEN previous_row_count = 0 THEN -1" in rendered.sql
    assert "current_row_count > previous_row_count * 1.2" in rendered.sql
    assert "current_row_count < previous_row_count * 0.8" in rendered.sql
    assert "DATE '2026-08-18'" in rendered.sql


def test_row_count_growth_one_sided_up() -> None:
    rendered = render(spec("row_count_growth", max_growth_ratio=0.2, direction="up"), CTX)
    assert "current_row_count > previous_row_count * 1.2" in rendered.sql
    assert "current_row_count < previous_row_count * 0.8" not in rendered.sql


def test_freshness_is_table_wide() -> None:
    rendered = render(spec("freshness", max_lag_days=2), CTX)
    assert 'max(CAST("date" AS DATE))' in rendered.sql
    assert "date_diff('day', max_partition, DATE '2026-08-19') > 2" in rendered.sql
    assert '"date" = DATE \'2026-08-19\'' not in rendered.sql


def test_scope_table_drops_partition_filter() -> None:
    ctx = RenderContext(**{**CTX.__dict__, "scope": "table"})
    rendered = render(spec("primary_key_unique"), ctx)
    assert '"date" = DATE' not in rendered.sql


def test_quote_literal_escapes_quotes_and_unicode() -> None:
    assert quote_literal("o'reilly") == "'o''reilly'"
    assert quote_literal("телефон") == "'телефон'"
    assert quote_literal(5) == "5"
    assert quote_literal(True) == "TRUE"


def main() -> int:
    test_primary_key_not_null()
    test_primary_key_unique()
    test_row_count_min_uses_non_strict_comparison()
    test_row_count_growth_two_sided_and_skips_without_baseline()
    test_row_count_growth_one_sided_up()
    test_freshness_is_table_wide()
    test_scope_table_drops_partition_filter()
    test_quote_literal_escapes_quotes_and_unicode()
    print("DQ SQL tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_sql.py`
Expected: FAIL с `ModuleNotFoundError: No module named 'dq.tests'`

- [ ] **Step 3: Реализовать `dq/tests.py` для базовых тестов**

```python
"""Рендер DQ-тестов в Trino SQL.

Каждый запрос возвращает ровно одну строку (failed_rows BIGINT, observed DOUBLE):
  failed_rows > 0  — тест упал, значение = число нарушающих строк или 1 для агрегатных тестов
  failed_rows = 0  — тест прошёл
  failed_rows < 0  — тест не мог быть выполнен, runner превращает это в статус skipped
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from dq.config import RenderContext, TestSpec


@dataclass(frozen=True)
class RenderedTest:
    spec: TestSpec
    test_key: str
    sql: str
    sample_sql: str | None
    threshold: str
    needs_baseline: bool = False


def quote_identifier(name: str) -> str:
    escaped = str(name).replace('"', '""')
    return f'"{escaped}"'


def quote_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def table_ref(ctx: RenderContext) -> str:
    return ".".join(
        (quote_identifier(ctx.catalog_alias), quote_identifier(ctx.schema), quote_identifier(ctx.table))
    )


def scope_predicate(ctx: RenderContext) -> str:
    if ctx.scope == "table":
        return "TRUE"
    return f"{quote_identifier(ctx.partition_column)} = DATE {quote_literal(ctx.partition_date.isoformat())}"


def _where(ctx: RenderContext, spec: TestSpec, violation: str) -> str:
    parts = [scope_predicate(ctx), f"({violation})"]
    if spec.where:
        parts.insert(1, f"({spec.where})")
    return " AND ".join(parts)


def _count_query(ctx: RenderContext, spec: TestSpec, violation: str) -> str:
    return (
        "SELECT count(*) AS failed_rows, CAST(count(*) AS DOUBLE) AS observed\n"
        f"FROM {table_ref(ctx)}\n"
        f"WHERE {_where(ctx, spec, violation)}"
    )


def _sample_query(ctx: RenderContext, spec: TestSpec, violation: str, extra_columns: tuple[str, ...] = ()) -> str:
    columns = list(dict.fromkeys(list(ctx.primary_key) + list(extra_columns)))
    projection = ", ".join(quote_identifier(column) for column in columns)
    return (
        f"SELECT {projection}\n"
        f"FROM {table_ref(ctx)}\n"
        f"WHERE {_where(ctx, spec, violation)}\n"
        f"LIMIT {int(ctx.sample_rows)}"
    )


def _render_primary_key_not_null(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    violation = " OR ".join(f"{quote_identifier(column)} IS NULL" for column in ctx.primary_key)
    return RenderedTest(
        spec=spec,
        test_key="primary_key_not_null",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation),
        threshold="0 rows with NULL primary key columns",
    )


def _render_primary_key_unique(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    return _render_unique(spec, ctx, ctx.primary_key, "primary_key_unique")


def _render_unique(spec: TestSpec, ctx: RenderContext, columns: tuple[str, ...], test_key: str) -> RenderedTest:
    projection = ", ".join(quote_identifier(column) for column in columns)
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    sql = (
        "SELECT count(*) AS failed_rows, CAST(count(*) AS DOUBLE) AS observed\n"
        "FROM (\n"
        f"  SELECT {projection}\n"
        f"  FROM {table_ref(ctx)}\n"
        f"  WHERE {' AND '.join(filters)}\n"
        f"  GROUP BY {projection}\n"
        "  HAVING count(*) > 1\n"
        ") AS duplicates"
    )
    sample_sql = (
        f"SELECT {projection}, count(*) AS duplicate_rows\n"
        f"FROM {table_ref(ctx)}\n"
        f"WHERE {' AND '.join(filters)}\n"
        f"GROUP BY {projection}\n"
        "HAVING count(*) > 1\n"
        f"LIMIT {int(ctx.sample_rows)}"
    )
    return RenderedTest(
        spec=spec,
        test_key=test_key,
        sql=sql,
        sample_sql=sample_sql,
        threshold="0 duplicate key groups",
    )


def _render_row_count_min(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    min_rows = int(spec.params["min_rows"])
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    sql = (
        f"SELECT CASE WHEN row_count <= {min_rows} THEN 1 ELSE 0 END AS failed_rows,\n"
        "       CAST(row_count AS DOUBLE) AS observed\n"
        "FROM (\n"
        f"  SELECT count(*) AS row_count FROM {table_ref(ctx)} WHERE {' AND '.join(filters)}\n"
        ") AS counted"
    )
    return RenderedTest(
        spec=spec,
        test_key="row_count_min",
        sql=sql,
        sample_sql=None,
        threshold=f"row_count > {min_rows}",
    )


def _render_row_count_growth(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    ratio = float(spec.params["max_growth_ratio"])
    direction = str(spec.params["direction"])
    baseline = ctx.partition_date - timedelta(days=1)
    column = quote_identifier(ctx.partition_column)
    current_literal = f"DATE {quote_literal(ctx.partition_date.isoformat())}"
    baseline_literal = f"DATE {quote_literal(baseline.isoformat())}"

    violations = []
    if direction in ("both", "up"):
        violations.append(f"current_row_count > previous_row_count * {1 + ratio}")
    if direction in ("both", "down"):
        violations.append(f"current_row_count < previous_row_count * {1 - ratio}")
    violation = " OR ".join(violations)

    sql = (
        "SELECT CASE WHEN previous_row_count = 0 THEN -1\n"
        f"            WHEN {violation} THEN 1\n"
        "            ELSE 0 END AS failed_rows,\n"
        "       CASE WHEN previous_row_count = 0 THEN NULL\n"
        "            ELSE CAST(current_row_count AS DOUBLE) / previous_row_count - 1 END AS observed\n"
        "FROM (\n"
        f"  SELECT count_if(CAST({column} AS DATE) = {current_literal}) AS current_row_count,\n"
        f"         count_if(CAST({column} AS DATE) = {baseline_literal}) AS previous_row_count\n"
        f"  FROM {table_ref(ctx)}\n"
        f"  WHERE CAST({column} AS DATE) IN ({current_literal}, {baseline_literal})\n"
        ") AS counts"
    )
    return RenderedTest(
        spec=spec,
        test_key="row_count_growth",
        sql=sql,
        sample_sql=None,
        threshold=f"|growth| <= {ratio} (direction={direction}, baseline={baseline.isoformat()})",
        needs_baseline=True,
    )


def _render_freshness(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    max_lag_days = int(spec.params["max_lag_days"])
    column = quote_identifier(ctx.partition_column)
    current_literal = f"DATE {quote_literal(ctx.partition_date.isoformat())}"
    sql = (
        "SELECT CASE WHEN max_partition IS NULL THEN 1\n"
        f"            WHEN date_diff('day', max_partition, {current_literal}) > {max_lag_days} THEN 1\n"
        "            ELSE 0 END AS failed_rows,\n"
        f"       CAST(date_diff('day', max_partition, {current_literal}) AS DOUBLE) AS observed\n"
        "FROM (\n"
        f"  SELECT max(CAST({column} AS DATE)) AS max_partition FROM {table_ref(ctx)}\n"
        ") AS bounds"
    )
    return RenderedTest(
        spec=spec,
        test_key="freshness",
        sql=sql,
        sample_sql=None,
        threshold=f"lag <= {max_lag_days} days",
    )


RENDERERS = {
    "primary_key_not_null": _render_primary_key_not_null,
    "primary_key_unique": _render_primary_key_unique,
    "row_count_min": _render_row_count_min,
    "row_count_growth": _render_row_count_growth,
    "freshness": _render_freshness,
}


def render(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    try:
        renderer = RENDERERS[spec.name]
    except KeyError as error:
        raise KeyError(f"Нет рендерера для DQ-теста {spec.name!r}") from error
    return renderer(spec, ctx)
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_dq_sql.py`
Expected: PASS, вывод `DQ SQL tests completed successfully`

- [ ] **Step 5: Коммит**

```bash
git add dq/tests.py ci_test/test_dq_sql.py
git commit -m "feat(dq): render base five DQ tests to Trino SQL"
```

---

### Task 3: Рендер SQL — тринадцать опциональных тестов

**Files:**
- Modify: `dq/tests.py`
- Modify: `ci_test/test_dq_sql.py`

**Interfaces:**
- Consumes: `dq.tests.render`, хелперы из Task 2.
- Produces: `RENDERERS` покрывает все 18 имён из `dq.config.TEST_PARAMS`.

- [ ] **Step 1: Написать падающие тесты**

Добавить в `ci_test/test_dq_sql.py` перед `main()`:

```python
def test_not_null_without_tolerance() -> None:
    rendered = render(spec("not_null", columns=["orders_cnt", "price_avg"], max_null_share=None), CTX)
    assert rendered.test_key == "not_null[orders_cnt,price_avg]"
    assert '"orders_cnt" IS NULL OR "price_avg" IS NULL' in rendered.sql


def test_not_null_with_tolerance_zeroes_failed_rows_below_share() -> None:
    rendered = render(spec("not_null", columns=["orders_cnt"], max_null_share=0.01), CTX)
    assert "> 0.01" in rendered.sql
    assert "ELSE 0 END AS failed_rows" in rendered.sql


def test_accepted_values_quotes_strings() -> None:
    rendered = render(spec("accepted_values", column="platform", values=["ios", "o'reilly"], ignore_nulls=True), CTX)
    assert "'o''reilly'" in rendered.sql
    assert '"platform" NOT IN' in rendered.sql
    assert '"platform" IS NOT NULL' in rendered.sql


def test_not_accepted_values() -> None:
    rendered = render(spec("not_accepted_values", column="platform", values=["unknown"], ignore_nulls=True), CTX)
    assert '"platform" IN (\'unknown\')' in rendered.sql


def test_accepted_range_inclusive_flags() -> None:
    rendered = render(
        spec("accepted_range", column="conversion_rate", min=0, max=1, min_inclusive=True, max_inclusive=False, ignore_nulls=True),
        CTX,
    )
    assert '"conversion_rate" < 0' in rendered.sql
    assert '"conversion_rate" >= 1' in rendered.sql
    assert rendered.test_key == "accepted_range[conversion_rate]"


def test_non_negative() -> None:
    rendered = render(spec("non_negative", columns=["orders_cnt"], ignore_nulls=True), CTX)
    assert '"orders_cnt" < 0' in rendered.sql


def test_null_share_below() -> None:
    rendered = render(spec("null_share_below", column="price_avg", max_share=0.05), CTX)
    assert "> 0.05" in rendered.sql


def test_string_not_blank() -> None:
    rendered = render(spec("string_not_blank", columns=["query"]), CTX)
    assert "trim(\"query\") = ''" in rendered.sql


def test_unique_combination() -> None:
    rendered = render(spec("unique_combination", columns=["date", "query"]), CTX)
    assert rendered.test_key == "unique_combination[date,query]"
    assert 'GROUP BY "date", "query"' in rendered.sql


def test_distinct_count_between() -> None:
    rendered = render(spec("distinct_count_between", columns=["sku_group_id"], min=1000, max=None), CTX)
    assert "distinct_count < 1000" in rendered.sql
    assert rendered.sample_sql is None


def test_columns_sum_equals() -> None:
    rendered = render(spec("columns_sum_equals", parts=["a", "b"], total="total_cnt", tolerance=1e-6), CTX)
    assert 'abs(("a" + "b") - "total_cnt") > 1e-06' in rendered.sql


def test_expression_is_true_treats_null_as_failure() -> None:
    rendered = render(spec("expression_is_true", expression="min_price <= max_price"), CTX)
    assert "(min_price <= max_price) IS NOT TRUE" in rendered.sql


def test_relationships_uses_not_exists() -> None:
    rendered = render(
        spec(
            "relationships",
            column="sku_group_id",
            to_table='"dwh-iceberg"."silver"."sku_dim"',
            to_column="sku_group_id",
            where=None,
        ),
        CTX,
    )
    assert "NOT EXISTS" in rendered.sql
    assert '"dwh-iceberg"."silver"."sku_dim"' in rendered.sql


def test_row_count_matches_reference_skips_without_reference_rows() -> None:
    rendered = render(
        spec(
            "row_count_matches_reference",
            reference_table='"dwh-iceberg"."silver"."upstream"',
            reference_date_column="event_date",
            reference_where=None,
            tolerance_ratio=0.0,
        ),
        CTX,
    )
    assert rendered.needs_baseline is True
    assert "WHEN reference_row_count = 0 THEN -1" in rendered.sql


def test_every_configured_test_has_a_renderer() -> None:
    from dq.config import TEST_PARAMS
    from dq.tests import RENDERERS

    assert set(RENDERERS) == set(TEST_PARAMS)
```

И дописать их вызовы в `main()` перед `print(...)`.

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_sql.py`
Expected: FAIL с `KeyError: "Нет рендерера для DQ-теста 'not_null'"`

- [ ] **Step 3: Дописать рендереры в `dq/tests.py`**

Добавить перед словарём `RENDERERS`:

```python
def _columns_key(name: str, columns: Any) -> str:
    return f"{name}[{','.join(str(column) for column in columns)}]"


def _null_violation(columns: tuple[str, ...]) -> str:
    return " OR ".join(f"{quote_identifier(column)} IS NULL" for column in columns)


def _render_not_null(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(spec.params["columns"])
    max_null_share = spec.params.get("max_null_share")
    violation = _null_violation(columns)
    test_key = _columns_key("not_null", columns)

    if max_null_share is None:
        return RenderedTest(
            spec=spec,
            test_key=test_key,
            sql=_count_query(ctx, spec, violation),
            sample_sql=_sample_query(ctx, spec, violation, columns),
            threshold="0 rows with NULL values",
        )

    share = float(max_null_share)
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    sql = (
        f"SELECT CASE WHEN null_share > {share} THEN null_rows ELSE 0 END AS failed_rows,\n"
        "       null_share AS observed\n"
        "FROM (\n"
        f"  SELECT count_if({violation}) AS null_rows,\n"
        f"         CAST(count_if({violation}) AS DOUBLE) / NULLIF(count(*), 0) AS null_share\n"
        f"  FROM {table_ref(ctx)}\n"
        f"  WHERE {' AND '.join(filters)}\n"
        ") AS shares"
    )
    return RenderedTest(
        spec=spec,
        test_key=test_key,
        sql=sql,
        sample_sql=_sample_query(ctx, spec, violation, columns),
        threshold=f"null share <= {share}",
    )


def _render_null_share_below(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = str(spec.params["column"])
    share = float(spec.params["max_share"])
    inner = TestSpec(
        name="not_null",
        family=spec.family,
        params={"columns": [column], "max_null_share": share},
        severity=spec.severity,
        where=spec.where,
    )
    rendered = _render_not_null(inner, ctx)
    return RenderedTest(
        spec=spec,
        test_key=f"null_share_below[{column}]",
        sql=rendered.sql,
        sample_sql=rendered.sample_sql,
        threshold=f"null share <= {share}",
    )


def _render_unique_combination(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    return _render_unique(spec, ctx, columns, _columns_key("unique_combination", columns))


def _values_list(values: Any) -> str:
    return ", ".join(quote_literal(value) for value in values)


def _render_accepted_values(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    violation = f"{column} NOT IN ({_values_list(spec.params['values'])})"
    if not spec.params.get("ignore_nulls", True):
        violation = f"{violation} OR {column} IS NULL"
    else:
        violation = f"{column} IS NOT NULL AND ({violation})"
    return RenderedTest(
        spec=spec,
        test_key=f"accepted_values[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"values in ({_values_list(spec.params['values'])})",
    )


def _render_not_accepted_values(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    violation = f"{column} IN ({_values_list(spec.params['values'])})"
    return RenderedTest(
        spec=spec,
        test_key=f"not_accepted_values[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"values not in ({_values_list(spec.params['values'])})",
    )


def _render_accepted_range(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    clauses = []
    if spec.params.get("min") is not None:
        operator = "<" if spec.params.get("min_inclusive", True) else "<="
        clauses.append(f"{column} {operator} {quote_literal(spec.params['min'])}")
    if spec.params.get("max") is not None:
        operator = ">" if spec.params.get("max_inclusive", True) else ">="
        clauses.append(f"{column} {operator} {quote_literal(spec.params['max'])}")
    violation = " OR ".join(clauses)
    if not spec.params.get("ignore_nulls", True):
        violation = f"({violation}) OR {column} IS NULL"
    return RenderedTest(
        spec=spec,
        test_key=f"accepted_range[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"{spec.params.get('min')} <= value <= {spec.params.get('max')}",
    )


def _render_non_negative(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    clauses = [f"{quote_identifier(column)} < 0" for column in columns]
    violation = " OR ".join(clauses)
    if not spec.params.get("ignore_nulls", True):
        violation = f"({violation}) OR ({_null_violation(columns)})"
    return RenderedTest(
        spec=spec,
        test_key=_columns_key("non_negative", columns),
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, columns),
        threshold="value >= 0",
    )


def _render_string_not_blank(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    clauses = []
    for column in columns:
        quoted = quote_identifier(column)
        clauses.append(f"{quoted} IS NULL OR trim({quoted}) = ''")
    violation = " OR ".join(clauses)
    return RenderedTest(
        spec=spec,
        test_key=_columns_key("string_not_blank", columns),
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, columns),
        threshold="non-empty trimmed string",
    )


def _render_distinct_count_between(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    projection = ", ".join(quote_identifier(column) for column in columns)
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    clauses = []
    if spec.params.get("min") is not None:
        clauses.append(f"distinct_count < {int(spec.params['min'])}")
    if spec.params.get("max") is not None:
        clauses.append(f"distinct_count > {int(spec.params['max'])}")
    violation = " OR ".join(clauses)
    sql = (
        f"SELECT CASE WHEN {violation} THEN 1 ELSE 0 END AS failed_rows,\n"
        "       CAST(distinct_count AS DOUBLE) AS observed\n"
        "FROM (\n"
        "  SELECT count(*) AS distinct_count FROM (\n"
        f"    SELECT DISTINCT {projection} FROM {table_ref(ctx)} WHERE {' AND '.join(filters)}\n"
        "  ) AS distinct_rows\n"
        ") AS counted"
    )
    return RenderedTest(
        spec=spec,
        test_key=_columns_key("distinct_count_between", columns),
        sql=sql,
        sample_sql=None,
        threshold=f"{spec.params.get('min')} <= distinct_count <= {spec.params.get('max')}",
    )


def _render_columns_sum_equals(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    parts = tuple(str(part) for part in spec.params["parts"])
    total = str(spec.params["total"])
    tolerance = float(spec.params.get("tolerance", 1e-6))
    summed = " + ".join(quote_identifier(part) for part in parts)
    violation = f"abs(({summed}) - {quote_identifier(total)}) > {tolerance}"
    return RenderedTest(
        spec=spec,
        test_key=f"columns_sum_equals[{total}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, parts + (total,)),
        threshold=f"|sum(parts) - {total}| <= {tolerance}",
    )


def _render_expression_is_true(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    expression = str(spec.params["expression"])
    # IS NOT TRUE ловит и FALSE, и NULL: выражение, не вычислившееся в TRUE, считается нарушением.
    violation = f"({expression}) IS NOT TRUE"
    return RenderedTest(
        spec=spec,
        test_key=f"expression_is_true[{expression[:40]}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation),
        threshold=f"({expression}) IS TRUE",
    )


def _render_relationships(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    to_table = str(spec.params["to_table"])
    to_column = quote_identifier(str(spec.params["to_column"]))
    reference_where = spec.params.get("where")
    reference_filter = f" AND ({reference_where})" if reference_where else ""
    violation = (
        f"{column} IS NOT NULL AND NOT EXISTS ("
        f"SELECT 1 FROM {to_table} AS reference "
        f"WHERE reference.{to_column} = {table_ref(ctx)}.{column}{reference_filter})"
    )
    return RenderedTest(
        spec=spec,
        test_key=f"relationships[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"every value exists in {to_table}",
    )


def _render_row_count_matches_reference(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    reference_table = str(spec.params["reference_table"])
    reference_date_column = quote_identifier(str(spec.params["reference_date_column"]))
    reference_where = spec.params.get("reference_where")
    tolerance = float(spec.params.get("tolerance_ratio", 0.0))
    current_literal = f"DATE {quote_literal(ctx.partition_date.isoformat())}"
    reference_filter = f" AND ({reference_where})" if reference_where else ""

    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")

    sql = (
        "SELECT CASE WHEN reference_row_count = 0 THEN -1\n"
        f"            WHEN abs(CAST(target_row_count AS DOUBLE) / reference_row_count - 1) > {tolerance} THEN 1\n"
        "            ELSE 0 END AS failed_rows,\n"
        "       CASE WHEN reference_row_count = 0 THEN NULL\n"
        "            ELSE CAST(target_row_count AS DOUBLE) / reference_row_count - 1 END AS observed\n"
        "FROM (\n"
        f"  SELECT (SELECT count(*) FROM {table_ref(ctx)} WHERE {' AND '.join(filters)}) AS target_row_count,\n"
        f"         (SELECT count(*) FROM {reference_table} "
        f"WHERE CAST({reference_date_column} AS DATE) = {current_literal}{reference_filter}) AS reference_row_count\n"
        ") AS counts"
    )
    return RenderedTest(
        spec=spec,
        test_key=f"row_count_matches_reference[{reference_table}]",
        sql=sql,
        sample_sql=None,
        threshold=f"|target/reference - 1| <= {tolerance}",
        needs_baseline=True,
    )
```

И расширить словарь:

```python
RENDERERS = {
    "primary_key_not_null": _render_primary_key_not_null,
    "primary_key_unique": _render_primary_key_unique,
    "row_count_min": _render_row_count_min,
    "row_count_growth": _render_row_count_growth,
    "freshness": _render_freshness,
    "not_null": _render_not_null,
    "null_share_below": _render_null_share_below,
    "unique_combination": _render_unique_combination,
    "accepted_values": _render_accepted_values,
    "not_accepted_values": _render_not_accepted_values,
    "accepted_range": _render_accepted_range,
    "non_negative": _render_non_negative,
    "string_not_blank": _render_string_not_blank,
    "distinct_count_between": _render_distinct_count_between,
    "columns_sum_equals": _render_columns_sum_equals,
    "row_count_matches_reference": _render_row_count_matches_reference,
    "expression_is_true": _render_expression_is_true,
    "relationships": _render_relationships,
}
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_dq_sql.py`
Expected: PASS

- [ ] **Step 5: Коммит**

```bash
git add dq/tests.py ci_test/test_dq_sql.py
git commit -m "feat(dq): render the thirteen optional DQ tests"
```

---

### Task 4: Runner — исполнение, warmup, active_from, preflight

**Files:**
- Create: `dq/runner.py`
- Test: `ci_test/test_dq_runner.py`

**Interfaces:**
- Consumes: `dq.config.DqSettings`, `dq.config.RenderContext`, `dq.tests.render`.
- Produces: `dq.runner.TestResult`, `dq.runner.DqRunOutcome` (поля `results: list[TestResult]`, `warmup_active: bool`, `skipped_by_active_from: bool`), `dq.runner.run_dq(settings, ctx, query) -> DqRunOutcome`, `dq.runner.preflight(query, ctx) -> None`, `dq.runner.DqPreflightError`.

`query` — вызываемый объект `(sql: str) -> list[tuple]`. Runner не знает про Airflow; в тестах передаётся фейк, в проде — обёртка над `TrinoHook.get_records`.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_dq_runner.py`:

```python
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext, load_dq_settings
from dq.runner import DqPreflightError, run_dq

CTX = RenderContext(
    catalog_alias="dwh-iceberg",
    schema="silver",
    table="feature_platform_sku_group_id_prices",
    primary_key=("date", "sku_group_id"),
    partition_column="date",
    partition_date=date(2026, 8, 19),
    scope="partition",
    sample_rows=5,
)

BASE_CONFIG = {
    "table": {"catalog": "iceberg", "schema": "silver", "name": "feature_platform_sku_group_id_prices", "primary_key": "date,sku_group_id"},
}


class FakeQuery:
    """Отдаёт заранее заданные ответы, сопоставляя их по подстроке в SQL."""

    def __init__(self, answers: list[tuple[str, list]], default: list | None = None) -> None:
        self.answers = answers
        self.default = default if default is not None else [(0, 0.0)]
        self.executed: list[str] = []

    def __call__(self, sql: str) -> list:
        self.executed.append(sql)
        for needle, rows in self.answers:
            if needle in sql:
                return rows
        return self.default


def test_all_passing_run() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery([("information_schema.tables", [(1,)]), ("COUNT(DISTINCT", [(30,)])])
    outcome = run_dq(settings, CTX, query)
    assert outcome.warmup_active is False
    assert [result.status for result in outcome.results] == ["passed"] * 5


def test_failed_test_carries_sample_and_severity() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery(
        [
            ("information_schema.tables", [(1,)]),
            ("COUNT(DISTINCT", [(30,)]),
            ("HAVING count(*) > 1\n) AS duplicates", [(3, 3.0)]),
            ("HAVING count(*) > 1\nLIMIT", [("2026-08-19", 118823, 2)]),
        ]
    )
    outcome = run_dq(settings, CTX, query)
    failed = [result for result in outcome.results if result.status == "failed"]
    assert len(failed) == 1
    assert failed[0].test_key == "primary_key_unique"
    assert failed[0].failed_rows == 3
    assert failed[0].severity == "error"
    assert "118823" in failed[0].sample


def test_negative_failed_rows_becomes_skipped() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery(
        [
            ("information_schema.tables", [(1,)]),
            ("COUNT(DISTINCT", [(30,)]),
            ("previous_row_count", [(-1, None)]),
        ]
    )
    outcome = run_dq(settings, CTX, query)
    growth = [result for result in outcome.results if result.test_key == "row_count_growth"][0]
    assert growth.status == "skipped"
    assert "2026-08-18" in growth.skip_reason


def test_warm_up_downgrades_error_to_warn() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery(
        [
            ("information_schema.tables", [(1,)]),
            ("COUNT(DISTINCT", [(0,)]),
            ("HAVING count(*) > 1\n) AS duplicates", [(3, 3.0)]),
            ("HAVING count(*) > 1\nLIMIT", []),
        ]
    )
    outcome = run_dq(settings, CTX, query)
    assert outcome.warmup_active is True
    unique = [result for result in outcome.results if result.test_key == "primary_key_unique"][0]
    assert unique.status == "warned"


def test_warn_severity_never_fails() -> None:
    config = {**BASE_CONFIG, "dq": {"tests": [{"name": "non_negative", "columns": ["orders_cnt"], "severity": "warn"}]}}
    settings = load_dq_settings(config)
    query = FakeQuery(
        [("information_schema.tables", [(1,)]), ("COUNT(DISTINCT", [(30,)]), ('"orders_cnt" < 0', [(7, 7.0)])]
    )
    outcome = run_dq(settings, CTX, query)
    non_negative = [result for result in outcome.results if result.name == "non_negative"][0]
    assert non_negative.status == "warned"


def test_active_from_skips_whole_run() -> None:
    config = {**BASE_CONFIG, "dq": {"active_from": "2026-09-01"}}
    settings = load_dq_settings(config)
    query = FakeQuery([("information_schema.tables", [(1,)])])
    outcome = run_dq(settings, CTX, query)
    assert outcome.skipped_by_active_from is True
    assert outcome.results == []


def test_missing_table_raises_diagnostic_error() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery([("information_schema.tables", [(0,)])])
    try:
        run_dq(settings, CTX, query)
    except DqPreflightError as error:
        message = str(error)
        assert "dwh-iceberg" in message
        assert "feature_platform_sku_group_id_prices" in message
        assert "миграц" in message
    else:
        raise AssertionError("preflight must fail when the table is missing")


def main() -> int:
    test_all_passing_run()
    test_failed_test_carries_sample_and_severity()
    test_negative_failed_rows_becomes_skipped()
    test_warm_up_downgrades_error_to_warn()
    test_warn_severity_never_fails()
    test_active_from_skips_whole_run()
    test_missing_table_raises_diagnostic_error()
    print("DQ runner tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_runner.py`
Expected: FAIL с `ModuleNotFoundError: No module named 'dq.runner'`

- [ ] **Step 3: Реализовать `dq/runner.py`**

```python
"""Исполнение DQ-тестов и сборка результатов."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from dq.config import DqSettings, RenderContext
from dq.tests import quote_identifier, quote_literal, render, table_ref

Query = Callable[[str], list]


class DqPreflightError(RuntimeError):
    """Таблица недоступна до начала проверок."""


@dataclass(frozen=True)
class TestResult:
    name: str
    test_key: str
    family: str
    status: str
    severity: str
    failed_rows: int
    observed: float | None
    threshold: str
    duration_ms: int
    sql: str
    sample: str = ""
    skip_reason: str = ""


@dataclass
class DqRunOutcome:
    results: list[TestResult] = field(default_factory=list)
    warmup_active: bool = False
    skipped_by_active_from: bool = False

    @property
    def has_errors(self) -> bool:
        return any(result.status == "failed" for result in self.results)


def preflight(query: Query, ctx: RenderContext) -> None:
    sql = (
        "SELECT count(*) FROM information_schema.tables\n"
        f"WHERE table_schema = {quote_literal(ctx.schema)} AND table_name = {quote_literal(ctx.table)}"
    )
    rows = query(sql)
    if not rows or int(rows[0][0]) == 0:
        raise DqPreflightError(
            f"Таблица {ctx.catalog_alias}.{ctx.schema}.{ctx.table} не найдена в каталоге Trino. "
            "DQ запускается только после того, как миграции применены; проверьте, что миграция "
            "этой энтити проехала на master."
        )


def _partition_history_count(query: Query, ctx: RenderContext) -> int:
    column = quote_identifier(ctx.partition_column)
    sql = (
        f"SELECT COUNT(DISTINCT CAST({column} AS DATE))\n"
        f"FROM {table_ref(ctx)}\n"
        f"WHERE CAST({column} AS DATE) < DATE {quote_literal(ctx.partition_date.isoformat())}"
    )
    rows = query(sql)
    return int(rows[0][0]) if rows and rows[0][0] is not None else 0


def _format_sample(rows: list) -> str:
    if not rows:
        return ""
    return " | ".join("(" + ", ".join(str(value) for value in row) + ")" for row in rows)


def run_dq(settings: DqSettings, ctx: RenderContext, query: Query) -> DqRunOutcome:
    outcome = DqRunOutcome()
    if not settings.enabled:
        return outcome

    preflight(query, ctx)

    if settings.active_from is not None and ctx.partition_date < settings.active_from:
        outcome.skipped_by_active_from = True
        return outcome

    if settings.warmup_days > 0:
        outcome.warmup_active = _partition_history_count(query, ctx) < settings.warmup_days

    for spec in settings.tests:
        rendered = render(spec, ctx)
        started = time.monotonic()
        rows = query(rendered.sql)
        duration_ms = int((time.monotonic() - started) * 1000)

        failed_rows = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        observed = float(rows[0][1]) if rows and len(rows[0]) > 1 and rows[0][1] is not None else None

        if failed_rows < 0:
            baseline = (ctx.partition_date - timedelta(days=1)).isoformat()
            outcome.results.append(
                _result(spec, rendered, "skipped", spec.severity, 0, observed, duration_ms,
                        skip_reason=f"no baseline data for {baseline}")
            )
            continue

        if failed_rows == 0:
            outcome.results.append(_result(spec, rendered, "passed", spec.severity, 0, observed, duration_ms))
            continue

        sample = ""
        if rendered.sample_sql:
            sample = _format_sample(query(rendered.sample_sql))

        effective_severity = "warn" if outcome.warmup_active else spec.severity
        status = "failed" if effective_severity == "error" else "warned"
        outcome.results.append(
            _result(spec, rendered, status, effective_severity, failed_rows, observed, duration_ms, sample=sample)
        )

    return outcome


def _result(
    spec: Any,
    rendered: Any,
    status: str,
    severity: str,
    failed_rows: int,
    observed: float | None,
    duration_ms: int,
    sample: str = "",
    skip_reason: str = "",
) -> TestResult:
    return TestResult(
        name=spec.name,
        test_key=rendered.test_key,
        family=spec.family,
        status=status,
        severity=severity,
        failed_rows=failed_rows,
        observed=observed,
        threshold=rendered.threshold,
        duration_ms=duration_ms,
        sql=rendered.sql,
        sample=sample,
        skip_reason=skip_reason,
    )
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_dq_runner.py`
Expected: PASS

- [ ] **Step 5: Коммит**

```bash
git add dq/runner.py ci_test/test_dq_runner.py
git commit -m "feat(dq): runner with preflight, warmup and skip semantics"
```

---

### Task 5: Отчёт для лога и для oncall

**Files:**
- Create: `dq/report.py`
- Test: `ci_test/test_dq_report.py`

**Interfaces:**
- Consumes: `dq.runner.TestResult`, `dq.runner.DqRunOutcome`, `dq.config.RenderContext`.
- Produces: `dq.report.format_log(outcome, ctx) -> str`, `dq.report.format_alert(outcome, ctx, log_url, limit=3500) -> str`.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_dq_report.py`:

```python
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from dq.report import format_alert, format_log
from dq.runner import DqRunOutcome, TestResult

CTX = RenderContext(
    catalog_alias="dwh-iceberg",
    schema="silver",
    table="feature_platform_sku_group_id_prices",
    primary_key=("date", "sku_group_id"),
    partition_column="date",
    partition_date=date(2026, 8, 19),
    scope="partition",
    sample_rows=5,
)


def make_outcome() -> DqRunOutcome:
    return DqRunOutcome(
        results=[
            TestResult("primary_key_not_null", "primary_key_not_null", "null_checks", "passed", "error", 0, 0.0, "0 rows", 1200, "SELECT 1"),
            TestResult("primary_key_unique", "primary_key_unique", "uniqueness", "failed", "error", 1843, 1843.0, "0 duplicate key groups", 8700, "SELECT dup", sample="(2026-08-19, 118823, 2)"),
            TestResult("accepted_range", "accepted_range[price]", "domain_values", "warned", "warn", 12, 12.0, "0 <= value <= 1e9", 900, "SELECT rng", sample="(2026-08-19, 940112)"),
            TestResult("row_count_growth", "row_count_growth", "consistency", "skipped", "error", 0, None, "|growth| <= 0.2", 400, "SELECT growth", skip_reason="no baseline data for 2026-08-18"),
        ]
    )


def test_log_contains_summary_and_failure_details() -> None:
    text = format_log(make_outcome(), CTX)
    assert "dwh-iceberg.silver.feature_platform_sku_group_id_prices" in text
    assert "date=2026-08-19" in text
    assert "warmup: off" in text
    assert "PASS  primary_key_not_null" in text
    assert "FAIL  primary_key_unique" in text
    assert "WARN  accepted_range[price]" in text
    assert "SKIP  row_count_growth" in text
    assert "no baseline data for 2026-08-18" in text
    assert "--- FAIL primary_key_unique ---" in text
    assert "samples   : (2026-08-19, 118823, 2)" in text
    assert "SELECT dup" in text


def test_log_marks_active_warmup() -> None:
    outcome = make_outcome()
    outcome.warmup_active = True
    assert "warmup: ACTIVE" in format_log(outcome, CTX)


def test_alert_lists_only_failed_and_warned_and_respects_limit() -> None:
    text = format_alert(make_outcome(), CTX, "https://airflow/log/1", limit=400)
    assert "feature_platform_sku_group_id_prices" in text
    assert "primary_key_unique" in text
    assert "https://airflow/log/1" in text
    assert "primary_key_not_null" not in text
    assert len(text) <= 400


def main() -> int:
    test_log_contains_summary_and_failure_details()
    test_log_marks_active_warmup()
    test_alert_lists_only_failed_and_warned_and_respects_limit()
    print("DQ report tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_report.py`
Expected: FAIL с `ModuleNotFoundError: No module named 'dq.report'`

- [ ] **Step 3: Реализовать `dq/report.py`**

```python
"""Форматирование результатов DQ для лога таски и для oncall-сообщения."""

from __future__ import annotations

from dq.config import RenderContext
from dq.runner import DqRunOutcome, TestResult

STATUS_LABEL = {"passed": "PASS", "failed": "FAIL", "warned": "WARN", "skipped": "SKIP", "errored": "ERR "}


def _table_fqn(ctx: RenderContext) -> str:
    return f"{ctx.catalog_alias}.{ctx.schema}.{ctx.table}"


def _summary_line(result: TestResult) -> str:
    label = STATUS_LABEL.get(result.status, result.status.upper())
    detail = result.skip_reason if result.status == "skipped" else f"{result.failed_rows} rows"
    return f"{label}  {result.test_key:<34} {detail:<34} {result.duration_ms / 1000:.1f}s  severity={result.severity}"


def format_log(outcome: DqRunOutcome, ctx: RenderContext) -> str:
    warmup = "ACTIVE (severity error понижен до warn)" if outcome.warmup_active else "off"
    lines = [
        f"DQ  {_table_fqn(ctx)}  date={ctx.partition_date.isoformat()}  scope={ctx.scope}  warmup: {warmup}"
    ]
    if outcome.skipped_by_active_from:
        lines.append("SKIP всего прогона: партиция раньше dq.active_from")
        return "\n".join(lines)

    lines.extend(_summary_line(result) for result in outcome.results)

    for result in outcome.results:
        if result.status not in ("failed", "warned"):
            continue
        lines.extend(
            [
                "",
                f"--- {STATUS_LABEL[result.status]} {result.test_key} ---",
                f"family    : {result.family}",
                f"threshold : {result.threshold}",
                f"observed  : {result.observed}",
                f"rows      : {result.failed_rows}",
                f"sql       : {result.sql}",
                f"samples   : {result.sample or '(нет — агрегатный тест)'}",
            ]
        )
    return "\n".join(lines)


def format_alert(outcome: DqRunOutcome, ctx: RenderContext, log_url: str, limit: int = 3500) -> str:
    problems = [result for result in outcome.results if result.status in ("failed", "warned")]
    failed = sum(1 for result in problems if result.status == "failed")
    warned = len(problems) - failed

    header = [
        f"DQ FAILED: {_table_fqn(ctx)}",
        f"партиция: {ctx.partition_date.isoformat()}  errors: {failed}  warnings: {warned}",
    ]
    if outcome.warmup_active:
        header.append("warmup активен — severity error понижен до warn")

    body = []
    for result in problems:
        body.append(
            f"{STATUS_LABEL[result.status]} {result.test_key}: {result.failed_rows} rows, "
            f"observed={result.observed}, threshold={result.threshold}"
        )
        if result.sample:
            body.append(f"    примеры: {result.sample}")

    footer = [f"лог: {log_url}"]

    text = "\n".join(header + body + footer)
    if len(text) <= limit:
        return text

    budget = limit - len("\n".join(header + footer)) - len("\n… отчёт обрезан\n")
    trimmed: list[str] = []
    used = 0
    for line in body:
        if used + len(line) + 1 > budget:
            break
        trimmed.append(line)
        used += len(line) + 1
    return "\n".join(header + trimmed + ["… отчёт обрезан"] + footer)[:limit]
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_dq_report.py`
Expected: PASS

- [ ] **Step 5: Коммит**

```bash
git add dq/report.py ci_test/test_dq_report.py
git commit -m "feat(dq): log and oncall report formatting"
```

---

### Task 6: Таблица результатов, миграция и обход конфигов в CI-скриптах

**Files:**
- Create: `dq/results/config.yaml`
- Create: `dq/results/migrations/create_table.sql`
- Modify: `.airflowignore`
- Modify: `scripts/run_pyspark_migrations.py:134`
- Modify: `scripts/sync_iceberg_maintenance.py:16`
- Modify: `ci_test/test_sync_iceberg_maintenance.py`
- Modify: `ci_test/test_run_pyspark_migrations.py`

**Interfaces:**
- Consumes: ничего из предыдущих задач.
- Produces: таблица `iceberg.silver.feature_platform_dq_results` с колонками, которые читает Task 7.

- [ ] **Step 1: Написать падающие тесты**

В `ci_test/test_sync_iceberg_maintenance.py` в `main()` после существующих ассертов на `discovered_tables` добавить:

```python
    assert "feature_platform_dq_results" in discovered_tables["silver"]
```

В `ci_test/test_run_pyspark_migrations.py` найти проверку списка обнаруженных миграций и добавить в `main()`:

```python
    migration_targets = {entry["table_name"] for entry in migrations.discover_migrations(Path("."))}
    assert "feature_platform_dq_results" in migration_targets
```

Если в файле нет функции с таким именем — использовать ту, что реально возвращает список миграций (посмотреть `scripts/run_pyspark_migrations.py:134`), и привести ассерт к её структуре.

- [ ] **Step 2: Запустить тесты и убедиться, что они падают**

Run: `python3 ci_test/test_sync_iceberg_maintenance.py`
Expected: FAIL с `AssertionError`

- [ ] **Step 3: Создать конфиг, миграцию и расширить обход**

`dq/results/config.yaml`:

```yaml
# Таблица истории DQ-прогонов. Пишется таской dq каждого DAG'а, читается Superset.
# create_dbt_pr: false — таблица не уезжает в dbt-trino и не заводит DQ-тесты сама на себя.
table:
  key: dq_results
  catalog: iceberg
  schema: silver
  name: feature_platform_dq_results
  primary_key: date,dag_id,test_key
  meta:
    team: team:search
    create_dbt_pr: false
    create_maintenance_pr: true
```

`dq/results/migrations/create_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Проверявшаяся партиция целевой таблицы',
    run_ts TIMESTAMP COMMENT 'Момент прогона DQ',
    dag_id STRING COMMENT 'DAG, внутри которого выполнялась таска dq',
    task_id STRING COMMENT 'Идентификатор таски, всегда dq',
    run_id STRING COMMENT 'Airflow run_id прогона',
    try_number INT COMMENT 'Номер попытки таски',
    catalog STRING COMMENT 'Каталог целевой таблицы из config.yaml',
    schema_name STRING COMMENT 'Схема целевой таблицы',
    table_name STRING COMMENT 'Имя целевой таблицы',
    test_name STRING COMMENT 'Имя теста из каталога DQ',
    test_key STRING COMMENT 'Уникальный ключ теста с параметрами, например accepted_range[price]',
    test_family STRING COMMENT 'Семейство теста: null_checks, uniqueness, domain_values, referential_integrity, row_expr, consistency, recency',
    status STRING COMMENT 'passed, failed, warned, skipped или errored',
    severity STRING COMMENT 'Эффективный severity прогона: error или warn',
    failed_rows BIGINT COMMENT 'Число нарушающих строк, для агрегатных тестов 1',
    observed DOUBLE COMMENT 'Наблюдаемое числовое значение: доля, коэффициент или счётчик',
    threshold STRING COMMENT 'Человекочитаемый порог теста',
    params STRING COMMENT 'JSON параметров теста',
    sql_text STRING COMMENT 'Отрендеренный SQL теста',
    sample STRING COMMENT 'Примеры нарушающих строк',
    duration_ms BIGINT COMMENT 'Длительность выполнения теста',
    skip_reason STRING COMMENT 'Причина статуса skipped',
    warmup_active BOOLEAN COMMENT 'Был ли активен warmup на момент прогона'
)
USING iceberg
COMMENT 'История прогонов DQ-тестов Feature Platform'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
```

В `.airflowignore` добавить строку:

```
dq/**
```

В `scripts/run_pyspark_migrations.py:134` заменить кортеж:

```python
    for config_root in ("layers", "datasets", "dq"):
```

В `scripts/sync_iceberg_maintenance.py:16` заменить константу:

```python
TABLE_CONFIG_ROOTS = ("layers", "datasets", "dq")
```

В `scripts/sync_dbt_sources.py` обход **не расширять** — таблица результатов не должна попадать в dbt-источники.

- [ ] **Step 4: Запустить проверки**

```bash
python3 ci_test/test_sync_iceberg_maintenance.py
python3 ci_test/test_run_pyspark_migrations.py
python3 ci_test/test_sync_dbt_sources.py
python3 scripts/validate_ranking_upload_configs.py
```
Expected: все четыре завершаются успешно; `test_sync_dbt_sources.py` подтверждает, что `feature_platform_dq_results` **не** появился в dbt-источниках.

- [ ] **Step 5: Коммит**

```bash
git add dq/results .airflowignore scripts/run_pyspark_migrations.py scripts/sync_iceberg_maintenance.py ci_test/test_sync_iceberg_maintenance.py ci_test/test_run_pyspark_migrations.py
git commit -m "feat(dq): results table, migration and config discovery in CI scripts"
```

---

### Task 7: Идемпотентная запись результатов в Iceberg

**Files:**
- Create: `dq/results.py`
- Test: `ci_test/test_dq_results.py`

**Interfaces:**
- Consumes: `dq.runner.DqRunOutcome`, `dq.config.RenderContext`.
- Produces: `dq.results.RunMeta` (поля `dag_id, task_id, run_id, try_number, run_ts`), `dq.results.build_rows(outcome, ctx, settings, meta) -> list[dict]`, `dq.results.write_results(repo_root, outcome, ctx, settings, meta) -> None`, `dq.results.results_table_ref(repo_root) -> tuple[str, str]`.

`build_rows` — чистая функция, тестируется офлайн. `write_results` тянет `pyiceberg` и вызывается только в Airflow.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_dq_results.py`:

```python
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext, load_dq_settings
from dq.results import RunMeta, build_rows, results_table_ref
from dq.runner import DqRunOutcome, TestResult

CTX = RenderContext(
    catalog_alias="dwh-iceberg",
    schema="silver",
    table="feature_platform_sku_group_id_prices",
    primary_key=("date", "sku_group_id"),
    partition_column="date",
    partition_date=date(2026, 8, 19),
    scope="partition",
    sample_rows=5,
)

META = RunMeta(
    dag_id="feature-platform.layers.silver.sku_group_id.sku_group_id_prices",
    task_id="dq",
    run_id="scheduled__2026-08-19T01:00:00+00:00",
    try_number=1,
    run_ts=datetime(2026, 8, 20, 1, 5, tzinfo=timezone.utc),
)


def test_results_table_ref_comes_from_config() -> None:
    assert results_table_ref(Path(".")) == ("silver", "feature_platform_dq_results")


def test_build_rows_maps_every_field() -> None:
    settings = load_dq_settings(
        {"table": {"catalog": "iceberg", "schema": "silver", "name": "feature_platform_sku_group_id_prices", "primary_key": "date,sku_group_id"}}
    )
    outcome = DqRunOutcome(
        results=[
            TestResult("row_count_min", "row_count_min", "consistency", "failed", "error", 1, 0.0, "row_count > 0", 1500, "SELECT 1", sample=""),
        ],
        warmup_active=True,
    )
    rows = build_rows(outcome, CTX, settings, META)
    assert len(rows) == 1
    row = rows[0]
    assert row["date"] == date(2026, 8, 19)
    assert row["dag_id"] == META.dag_id
    assert row["task_id"] == "dq"
    assert row["try_number"] == 1
    assert row["catalog"] == "dwh-iceberg"
    assert row["schema_name"] == "silver"
    assert row["table_name"] == "feature_platform_sku_group_id_prices"
    assert row["test_family"] == "consistency"
    assert row["status"] == "failed"
    assert row["failed_rows"] == 1
    assert row["warmup_active"] is True
    assert json.loads(row["params"])["min_rows"] == 0


def test_build_rows_is_empty_when_run_skipped_by_active_from() -> None:
    settings = load_dq_settings(
        {
            "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
            "dq": {"active_from": "2026-09-01"},
        }
    )
    outcome = DqRunOutcome(skipped_by_active_from=True)
    assert build_rows(outcome, CTX, settings, META) == []


def main() -> int:
    test_results_table_ref_comes_from_config()
    test_build_rows_maps_every_field()
    test_build_rows_is_empty_when_run_skipped_by_active_from()
    print("DQ results tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_results.py`
Expected: FAIL с `ModuleNotFoundError: No module named 'dq.results'`

- [ ] **Step 3: Реализовать `dq/results.py`**

```python
"""Идемпотентная запись истории DQ-прогонов в Iceberg."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from dq.config import DqSettings, RenderContext
from dq.runner import DqRunOutcome

RESULTS_CONFIG_PATH = Path("dq") / "results" / "config.yaml"


@dataclass(frozen=True)
class RunMeta:
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    run_ts: datetime


def results_table_ref(repo_root: Path) -> tuple[str, str]:
    """PyIceberg-идентификатор таблицы результатов: ровно (schema, name)."""
    config = yaml.safe_load((Path(repo_root) / RESULTS_CONFIG_PATH).read_text(encoding="utf-8"))
    table = config["table"]
    schema = str(table["schema"]).strip()
    name = str(table["name"]).strip()
    if not schema or not name:
        raise ValueError(f"{RESULTS_CONFIG_PATH}: table.schema и table.name обязательны")
    return schema, name


def build_rows(
    outcome: DqRunOutcome,
    ctx: RenderContext,
    settings: DqSettings,
    meta: RunMeta,
) -> list[dict[str, Any]]:
    params_by_key = {spec.name: spec.params for spec in settings.tests}
    rows: list[dict[str, Any]] = []
    for result in outcome.results:
        rows.append(
            {
                "date": ctx.partition_date,
                "run_ts": meta.run_ts,
                "dag_id": meta.dag_id,
                "task_id": meta.task_id,
                "run_id": meta.run_id,
                "try_number": int(meta.try_number),
                "catalog": ctx.catalog_alias,
                "schema_name": ctx.schema,
                "table_name": ctx.table,
                "test_name": result.name,
                "test_key": result.test_key,
                "test_family": result.family,
                "status": result.status,
                "severity": result.severity,
                "failed_rows": int(result.failed_rows),
                "observed": result.observed,
                "threshold": result.threshold,
                "params": json.dumps(params_by_key.get(result.name, {}), ensure_ascii=False, default=str),
                "sql_text": result.sql,
                "sample": result.sample,
                "duration_ms": int(result.duration_ms),
                "skip_reason": result.skip_reason,
                "warmup_active": bool(outcome.warmup_active),
            }
        )
    return rows


def write_results(
    repo_root: Path,
    outcome: DqRunOutcome,
    ctx: RenderContext,
    settings: DqSettings,
    meta: RunMeta,
) -> None:
    rows = build_rows(outcome, ctx, settings, meta)
    if not rows:
        return

    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import And, EqualTo

    schema, name = results_table_ref(repo_root)
    catalog = load_catalog("iceberg")
    table = catalog.load_table((schema, name))

    arrow_table = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    table.overwrite(
        arrow_table,
        overwrite_filter=And(
            EqualTo("date", ctx.partition_date),
            EqualTo("dag_id", meta.dag_id),
        ),
    )
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_dq_results.py`
Expected: PASS

- [ ] **Step 5: Коммит**

```bash
git add dq/results.py ci_test/test_dq_results.py
git commit -m "feat(dq): idempotent write of DQ run history to Iceberg"
```

---

### Task 8: Airflow-таска `dq`

**Files:**
- Create: `dq/task.py`
- Test: `ci_test/test_dq_task.py`

**Interfaces:**
- Consumes: всё из Task 1–7.
- Produces: `dq.task.build_dq_task(config_path: str, repo_root: str) -> Callable`, `dq.task.build_render_context(config, repo_root, partition_date) -> RenderContext`, `dq.task.DqTestsFailed`.

`build_dq_task` возвращает Airflow-таску с `task_id="dq"`. Импорт `airflow` происходит **внутри** `build_dq_task`, чтобы `build_render_context` можно было тестировать офлайн.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_dq_task.py`:

```python
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

from dq.task import build_render_context


def test_render_context_from_entity_config() -> None:
    config = yaml.safe_load(
        Path("layers/silver/sku_group_id/sku_group_id_prices/v1/config.yaml").read_text(encoding="utf-8")
    )
    ctx = build_render_context(config, Path("."), date(2026, 8, 19))
    assert ctx.catalog_alias == "dwh-iceberg"
    assert ctx.schema == "silver"
    assert ctx.table == "feature_platform_sku_group_id_prices"
    assert ctx.primary_key == ("date", "sku_group_id")
    assert ctx.partition_column == "date"
    assert ctx.partition_date == date(2026, 8, 19)
    assert ctx.scope == "partition"


def main() -> int:
    test_render_context_from_entity_config()
    print("DQ task tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_task.py`
Expected: FAIL с `ModuleNotFoundError: No module named 'dq.task'`

- [ ] **Step 3: Реализовать `dq/task.py`**

```python
"""Фабрика Airflow-таски dq."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Callable

import yaml

from dq.config import RenderContext, load_dq_settings, trino_catalog_alias
from dq.report import format_alert, format_log
from dq.results import RunMeta, write_results
from dq.runner import run_dq

TASK_ID = "dq"


class DqTestsFailed(Exception):
    """Хотя бы один DQ-тест с severity error не прошёл."""


def build_render_context(config: dict[str, Any], repo_root: Path, partition_date: date) -> RenderContext:
    table = config["table"]
    settings = load_dq_settings(config)
    return RenderContext(
        catalog_alias=trino_catalog_alias(repo_root, str(table["catalog"])),
        schema=str(table["schema"]),
        table=str(table["name"]),
        primary_key=tuple(column.strip() for column in str(table["primary_key"]).split(",") if column.strip()),
        partition_column=settings.partition_column,
        partition_date=partition_date,
        scope=settings.scope,
        sample_rows=settings.sample_rows,
    )


def build_dq_task(config_path: str, repo_root: str) -> Callable:
    """Возвращает готовую Airflow-таску dq для DAG'а энтити."""
    from airflow.providers.trino.hooks.trino import TrinoHook
    from airflow.sdk import get_current_context, task
    from airflow_commons.helpers.oncall import send_oncall_notification

    config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    settings = load_dq_settings(config)
    alerts = config["alerts"]

    @task(
        task_id=TASK_ID,
        retries=1,
        on_failure_callback=send_oncall_notification(
            team=alerts["team"],
            oncall_webhook_conn_id=alerts["oncall_webhook_conn_id"],
            severity=alerts["severity"],
        ),
    )
    def dq(partition_date_value: str) -> None:
        import logging

        logger = logging.getLogger("airflow.task")
        partition_date = date.fromisoformat(str(partition_date_value)[:10])
        ctx = build_render_context(config, Path(repo_root), partition_date)

        hook = TrinoHook(trino_conn_id=settings.trino_conn_id)

        def query(sql: str) -> list:
            logger.info("DQ query:\n%s", sql)
            return hook.get_records(sql)

        outcome = run_dq(settings, ctx, query)
        logger.info("\n%s", format_log(outcome, ctx))

        airflow_context = get_current_context()
        task_instance = airflow_context["task_instance"]
        write_results(
            Path(repo_root),
            outcome,
            ctx,
            settings,
            RunMeta(
                dag_id=task_instance.dag_id,
                task_id=TASK_ID,
                run_id=task_instance.run_id,
                try_number=int(task_instance.try_number),
                run_ts=airflow_context["logical_date"],
            ),
        )

        if outcome.has_errors:
            log_url = getattr(task_instance, "log_url", "")
            raise DqTestsFailed(format_alert(outcome, ctx, log_url))

    return dq
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_dq_task.py`
Expected: PASS

- [ ] **Step 5: Коммит**

```bash
git add dq/task.py ci_test/test_dq_task.py
git commit -m "feat(dq): airflow task factory for the dq task"
```

---

### Task 9: CI-валидатор конфигов и шаг в Drone

**Files:**
- Create: `scripts/validate_dq_configs.py`
- Modify: `.drone.yaml:22-26`
- Test: `ci_test/test_validate_dq_configs.py`

**Interfaces:**
- Consumes: `dq.config.load_dq_settings`, `dq.config.DqConfigError`.
- Produces: `scripts/validate_dq_configs.py` с `discover_entity_configs(repo_root) -> list[Path]`, `validate_config(config_path, repo_root) -> list[str]`, `main() -> int`.

Валидатор проверяет: блок `dq:` парсится; все колонки, упомянутые в параметрах тестов, присутствуют в миграциях энтити; `dq.enabled: false` сопровождается объяснением в README.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_validate_dq_configs.py`:

```python
import importlib.util
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def load_validator():
    module_path = Path("scripts/validate_dq_configs.py")
    spec = importlib.util.spec_from_file_location("validate_dq_configs", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ENTITY_CONFIG = """table:
  catalog: iceberg
  schema: silver
  name: feature_platform_demo
  primary_key: date,sku_group_id
  meta:
    team: team:search
alerts:
  team: search
  severity: P3
  oncall_webhook_conn_id: oncall_webhook_search
dq:
  tests:
    - name: non_negative
      columns: [orders_cnt]
"""

MIGRATION = """CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'd',
    sku_group_id BIGINT COMMENT 's',
    orders_cnt BIGINT COMMENT 'o'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
"""


def write_entity(repo: Path, config_text: str, migration_text: str = MIGRATION) -> Path:
    entity = repo / "layers/silver/sku_group_id/demo/v1"
    (entity / "migrations").mkdir(parents=True)
    (entity / "config.yaml").write_text(config_text, encoding="utf-8")
    (entity / "migrations/create_table.sql").write_text(migration_text, encoding="utf-8")
    (entity / "README.md").write_text("# demo\n", encoding="utf-8")
    (repo / "ci_config.yaml").write_text('{"dbt": {"database_mapping": {"iceberg": "dwh-iceberg"}}}', encoding="utf-8")
    return entity / "config.yaml"


def test_valid_config_has_no_problems() -> None:
    validator = load_validator()
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, ENTITY_CONFIG)
        assert validator.validate_config(config_path, repo) == []


def test_unknown_column_is_reported() -> None:
    validator = load_validator()
    bad = ENTITY_CONFIG.replace("orders_cnt", "no_such_column")
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, bad)
        problems = validator.validate_config(config_path, repo)
        assert any("no_such_column" in problem for problem in problems)


def test_unknown_test_name_is_reported() -> None:
    validator = load_validator()
    bad = ENTITY_CONFIG.replace("non_negative", "definitely_not_a_test")
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, bad)
        problems = validator.validate_config(config_path, repo)
        assert any("definitely_not_a_test" in problem for problem in problems)


def test_disabled_dq_without_readme_explanation_is_reported() -> None:
    validator = load_validator()
    disabled = ENTITY_CONFIG.replace("dq:\n  tests:", "dq:\n  enabled: false\n  tests:")
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, disabled)
        problems = validator.validate_config(config_path, repo)
        assert any("README" in problem for problem in problems)


def test_repository_configs_are_all_valid() -> None:
    validator = load_validator()
    repo = Path(".")
    problems: list[str] = []
    for config_path in validator.discover_entity_configs(repo):
        problems.extend(validator.validate_config(config_path, repo))
    assert problems == [], problems


def main() -> int:
    test_valid_config_has_no_problems()
    test_unknown_column_is_reported()
    test_unknown_test_name_is_reported()
    test_disabled_dq_without_readme_explanation_is_reported()
    test_repository_configs_are_all_valid()
    print("DQ config validator tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_validate_dq_configs.py`
Expected: FAIL с `RuntimeError: Cannot load scripts/validate_dq_configs.py`

- [ ] **Step 3: Реализовать `scripts/validate_dq_configs.py`**

```python
#!/usr/bin/env python3
"""CI-гейт: проверяет корректность блоков dq: во всех энтити-конфигах."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import DqConfigError, load_dq_settings  # noqa: E402

ENTITY_CONFIG_ROOTS = ("layers", "datasets")
COLUMN_PARAMS = ("columns", "parts")
SINGLE_COLUMN_PARAMS = ("column", "total")
COLUMN_DEFINITION = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+[A-Za-z]", re.MULTILINE)
ADD_COLUMN = re.compile(r"ADD\s+COLUMNS?\s*\(?\s*([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)


def discover_entity_configs(repo_root: Path) -> list[Path]:
    configs: list[Path] = []
    for config_root in ENTITY_CONFIG_ROOTS:
        for config_path in sorted(Path(repo_root).glob(f"{config_root}/**/config.yaml")):
            configs.append(config_path)
    return configs


def migration_columns(entity_dir: Path) -> set[str]:
    columns: set[str] = set()
    migrations_dir = entity_dir / "migrations"
    if not migrations_dir.is_dir():
        return columns
    for sql_path in sorted(migrations_dir.glob("*.sql")):
        text = sql_path.read_text(encoding="utf-8")
        columns.update(match.lower() for match in COLUMN_DEFINITION.findall(text))
        columns.update(match.lower() for match in ADD_COLUMN.findall(text))
    return columns


def validate_config(config_path: Path, repo_root: Path) -> list[str]:
    problems: list[str] = []
    config: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if "table" not in config:
        return problems

    try:
        settings = load_dq_settings(config)
    except DqConfigError as error:
        return [f"{config_path}: {error}"]

    entity_dir = config_path.parent
    if not settings.enabled:
        readme = entity_dir / "README.md"
        readme_text = readme.read_text(encoding="utf-8") if readme.is_file() else ""
        if "dq" not in readme_text.lower():
            problems.append(
                f"{config_path}: dq.enabled=false, но в README нет объяснения, почему DQ выключен"
            )
        return problems

    known_columns = migration_columns(entity_dir)
    if not known_columns:
        return problems

    for spec in settings.tests:
        referenced: list[str] = []
        for key in COLUMN_PARAMS:
            value = spec.params.get(key)
            if isinstance(value, list):
                referenced.extend(str(item) for item in value)
        for key in SINGLE_COLUMN_PARAMS:
            value = spec.params.get(key)
            if isinstance(value, str):
                referenced.append(value)
        for column in referenced:
            if column.lower() not in known_columns:
                problems.append(
                    f"{config_path}: тест {spec.name} ссылается на колонку {column!r}, "
                    f"которой нет в миграциях энтити"
                )
    return problems


def main() -> int:
    repo_root = Path(".")
    problems: list[str] = []
    configs = discover_entity_configs(repo_root)
    for config_path in configs:
        problems.extend(validate_config(config_path, repo_root))

    print(f"Проверено конфигов: {len(configs)}")
    if problems:
        for problem in problems:
            print(f"ERROR {problem}")
        return 1
    print("Все блоки dq: валидны")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

```bash
python3 ci_test/test_validate_dq_configs.py
python3 scripts/validate_dq_configs.py
```
Expected: оба завершаются успешно.

- [ ] **Step 5: Добавить шаг в Drone**

В `.drone.yaml` сразу после шага `validate ranking upload configs` (строки 22–26) вставить:

```yaml
  - name: validate dq configs
    image: python:3.11-slim
    commands:
      - echo "=== validate dq configs ==="
      - pip install --no-cache-dir PyYAML==6.0.2
      - python scripts/validate_dq_configs.py
```

- [ ] **Step 6: Коммит**

```bash
git add scripts/validate_dq_configs.py ci_test/test_validate_dq_configs.py .drone.yaml
git commit -m "feat(dq): CI validator for dq config blocks"
```

---

### Task 10: Пилот на Spark-энтити `sku_group_id_prices`

**Files:**
- Modify: `layers/silver/sku_group_id/sku_group_id_prices/v1/config.yaml`
- Modify: `layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py`
- Modify: `layers/silver/sku_group_id/sku_group_id_prices/v1/README.md`
- Test: `ci_test/test_dq_task_wiring.py`

**Interfaces:**
- Consumes: `dq.task.build_dq_task`.
- Produces: DAG `feature-platform.layers.silver.sku_group_id.sku_group_id_prices` с терминальной таской `dq`.

Партиция этой энтити выводится из `data_interval_start` (см. `config/factory.py:175`), поэтому шаблон даты — `{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}`.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_dq_task_wiring.py`:

```python
import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PILOT_DAGS = (
    "layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py",
    "layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py",
)


def uses_build_dq_task(dag_path: Path) -> bool:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "build_dq_task":
            return True
    return False


def test_pilot_dags_build_the_dq_task() -> None:
    for relative in PILOT_DAGS:
        dag_path = Path(relative)
        assert dag_path.is_file(), relative
        assert uses_build_dq_task(dag_path), f"{relative}: нет вызова build_dq_task"


def test_pilot_dags_declare_dq_as_terminal_task() -> None:
    for relative in PILOT_DAGS:
        text = Path(relative).read_text(encoding="utf-8")
        assert ">> dq_task" in text, f"{relative}: таска dq не подключена в конце графа"
        assert "dq_task >>" not in text, f"{relative}: таска dq не должна иметь downstream внутри DAG'а"


def main() -> int:
    test_pilot_dags_build_the_dq_task()
    test_pilot_dags_declare_dq_as_terminal_task()
    print("DQ task wiring tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_dq_task_wiring.py`
Expected: FAIL с `AssertionError: layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py: нет вызова build_dq_task`

- [ ] **Step 3: Добавить блок `dq:` в конфиг энтити**

В конец `layers/silver/sku_group_id/sku_group_id_prices/v1/config.yaml` дописать:

```yaml
dq:
  trino_conn_id: trino_search
  partition_date_template: '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  tests:
    # Пилот: двусторонний growth впервые начнёт ловить обвалы объёма,
    # поэтому первые две недели держим его предупреждением.
    - name: row_count_growth
      max_growth_ratio: 0.2
      severity: warn
    - name: non_negative
      columns:
        - avg_sell_price_eod
        - median_sell_price_eod
        - min_sell_price_eod
        - max_sell_price_eod
        - avg_full_price_eod
        - median_full_price_eod
        - min_full_price_eod
        - max_full_price_eod
    - name: expression_is_true
      expression: "min_sell_price_eod <= median_sell_price_eod AND median_sell_price_eod <= max_sell_price_eod"
    - name: expression_is_true
      expression: "min_full_price_eod <= median_full_price_eod AND median_full_price_eod <= max_full_price_eod"
```

- [ ] **Step 4: Подключить таску в DAG**

В `layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py` после строки `sys.path.insert(0, DAG_DIR)` добавить:

```python
REPO_ROOT = os.path.abspath(os.path.join(DAG_DIR, "..", "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task

CONFIG_PATH = os.path.join(DAG_DIR, "config.yaml")
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
```

Внутри `collect_silver_sku_group_id_prices()` заменить последнюю строку `wait_for_sku_eod >> collect_prices` на:

```python
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    wait_for_sku_eod >> collect_prices >> dq_task
```

- [ ] **Step 5: Обновить README энтити**

В `layers/silver/sku_group_id/sku_group_id_prices/v1/README.md` добавить раздел:

```markdown
## DQ

DQ-тесты выполняются таской `dq` внутри этого DAG'а сразу после записи партиции; каталог
тестов и правила конфигурирования описаны в `dq/README.md`.

Базовый набор: `primary_key_not_null`, `primary_key_unique`, `row_count_min`,
`row_count_growth`, `freshness`.

Дополнительно: `non_negative` по всем ценовым колонкам и два `expression_is_true`,
проверяющих упорядоченность `min <= median <= max` отдельно для sell- и full-цен.

`row_count_growth` временно имеет `severity: warn`: двусторонняя проверка роста впервые
начинает ловить обвалы объёма, которых прежний односторонний dbt-тест не видел. Поднять до
`error` после двух недель наблюдений.

Downstream ждёт таску `dq` этого DAG'а, а не dbt-DQ-DAG.
```

- [ ] **Step 6: Запустить проверки**

```bash
python3 ci_test/test_dq_task_wiring.py
python3 scripts/validate_dq_configs.py
python3 ci_test/test_dq_config.py
git diff --check
```
Expected: `test_dq_task_wiring.py` падает только на втором пилоте (`search_query_sku_group_es_features`), остальное проходит.

- [ ] **Step 7: Коммит**

```bash
git add layers/silver/sku_group_id/sku_group_id_prices/v1 ci_test/test_dq_task_wiring.py
git commit -m "feat(dq): pilot dq task in sku_group_id_prices spark DAG"
```

---

### Task 11: Пилот на Airflow/Python-энтити `search_query_sku_group_es_features`

**Files:**
- Modify: `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/config.yaml`
- Modify: `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py`
- Modify: `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/README.md`

**Interfaces:**
- Consumes: `dq.task.build_dq_task`.
- Produces: DAG `feature-platform.layers.silver.query_sku_group_id.search_query_sku_group_es_features` с терминальной таской `dq`.

У этого DAG'а `schedule=None`, а партиция приходит из `dag_run.conf["partition_date"]` с фолбэком на `macros.ds_add(ds, -1)` — то же выражение уже используется для `prepare_parquet` и `load_to_iceberg` (`dag.py:169`).

- [ ] **Step 1: Убедиться, что тест из Task 10 всё ещё падает на этой энтити**

Run: `python3 ci_test/test_dq_task_wiring.py`
Expected: FAIL с `AssertionError: layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py: нет вызова build_dq_task`

- [ ] **Step 2: Добавить блок `dq:` в конфиг энтити**

В конец `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/config.yaml` дописать:

```yaml
dq:
  trino_conn_id: trino_search
  partition_date_template: '{{ (dag_run.conf or {}).get("partition_date") or macros.ds_add(ds, -1) }}'
  # Таблица наполняется нерегулярными ручными запусками, поэтому объём между днями
  # прыгает штатно, и growth здесь только предупреждает.
  tests:
    - name: row_count_growth
      max_growth_ratio: 0.5
      severity: warn
    - name: string_not_blank
      columns:
        - query
```

- [ ] **Step 3: Подключить таску в DAG**

В `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py` после строки `JOB_DIR = os.path.join(ENTITY_DIR, "job")` добавить:

```python
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
```

В конце функции `search_query_sku_group_es_features_dag()` заменить блок

```python
    prepared = prepare_parquet(partition_date_arg)
    loaded = load_to_iceberg(partition_date_arg)
    wait_for_elasticsearch_collect >> prepared >> loaded
```

на

```python
    prepared = prepare_parquet(partition_date_arg)
    loaded = load_to_iceberg(partition_date_arg)
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(partition_date_arg)
    wait_for_elasticsearch_collect >> prepared >> loaded >> dq_task
```

- [ ] **Step 4: Обновить README энтити**

В `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/README.md` добавить раздел:

```markdown
## DQ

DQ-тесты выполняются таской `dq` внутри этого DAG'а сразу после `load_to_iceberg`; каталог
тестов и правила конфигурирования описаны в `dq/README.md`.

Базовый набор: `primary_key_not_null`, `primary_key_unique`, `row_count_min`,
`row_count_growth`, `freshness`. Дополнительно: `string_not_blank` по колонке `query`.

`row_count_growth` имеет `severity: warn` и повышенный порог `0.5`: DAG запускается вручную
и нерегулярно, поэтому объём между соседними партициями штатно прыгает.

Партиция для DQ берётся тем же выражением, что и для записи:
`dag_run.conf["partition_date"]` с фолбэком на `macros.ds_add(ds, -1)`.

Downstream ждёт таску `dq` этого DAG'а, а не dbt-DQ-DAG.
```

- [ ] **Step 5: Запустить проверки**

```bash
python3 ci_test/test_dq_task_wiring.py
python3 scripts/validate_dq_configs.py
git diff --check
```
Expected: всё проходит.

- [ ] **Step 6: Коммит**

```bash
git add layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1
git commit -m "feat(dq): pilot dq task in search_query_sku_group_es_features DAG"
```

---

### Task 12: Документация — `dq/README.md` и `AGENTS.md`

**Files:**
- Create: `dq/README.md`
- Modify: `AGENTS.md` (разделы «First Rules», «Layer Layout», «DQ And Source Sync», «CI Contracts», «Local Validation Commands»)

**Interfaces:**
- Consumes: имена тестов и параметры из `dq/config.py`.
- Produces: письменный контракт, на который ссылаются README энтити из Task 10–11.

- [ ] **Step 1: Написать `dq/README.md`**

Файл должен содержать: назначение пакета; полную таблицу 18 тестов с семейством, параметрами и дефолтами (скопировать из §6 спеки); правила `scope`, `warmup_days`, `active_from`; объяснение статуса `skipped`; предупреждение про дороговизну `relationships` и `row_count_matches_reference`; описание таблицы `feature_platform_dq_results`; пример полного блока `dq:`; отличия от прежнего поведения dbt (таблица из §15 спеки); указание, что `expression_is_true` трактует NULL-результат выражения как нарушение (`IS NOT TRUE`).

- [ ] **Step 2: Обновить `AGENTS.md`**

В «First Rules» добавить пункт:

```markdown
- DQ-тесты репозиторно-управляемых таблиц объявляются в блоке `dq:` файла `config.yaml` энтити и исполняются таской `dq` внутри того же DAG'а сразу после записи партиции. Общий код DQ живёт только в top-level пакете `dq/`; не создавайте `layers/_common` и не дублируйте DQ-логику внутри энтити. Каталог тестов и правила конфигурирования — в `dq/README.md`.
```

В «Layer Layout» после строки про `README.md` добавить:

```markdown
- Таска `dq`: терминальная таска DAG'а, собираемая `dq.task.build_dq_task(config_path, repo_root)`. Downstream-DAG'и ждут именно её через `external_task_id="dq"`.
```

В «DQ And Source Sync» заменить первые четыре пункта на:

```markdown
- Каждая репозиторно-управляемая энтити получает DQ-тесты внутри собственного DAG'а, таской `dq`.
- Базовый набор (`primary_key_not_null`, `primary_key_unique`, `row_count_min`, `row_count_growth`, `freshness`) работает всегда, даже без блока `dq:` в конфиге.
- Дополнительные тесты объявляются именами в `dq.tests`; полный каталог — в `dq/README.md`.
- Downstream-DAG'и ждут таску `dq` DAG'а-владельца таблицы: `external_dag_id=<dag id владельца>`, `external_task_id="dq"`. Ссылки на `dbt.source.trino.ml_feature_platform_*.dq` — устаревший контракт, они убираются на фазе 3 миграции.
- Для upstream-таблиц, принадлежащих DE, по-прежнему используется DAG/DQ-контракт производящей команды.
```

В «CI Contracts» в список того, что делает Drone, добавить первым пунктом:

```markdown
- Runs `scripts/validate_dq_configs.py`.
```

и в блоке про обход конфигов заменить упоминание корней на `layers/**`, `datasets/**` и `dq/**`.

В «Local Validation Commands» добавить в блок команд:

```bash
python3 scripts/validate_dq_configs.py
python3 ci_test/test_dq_config.py
python3 ci_test/test_dq_sql.py
python3 ci_test/test_dq_runner.py
python3 ci_test/test_dq_report.py
python3 ci_test/test_dq_results.py
python3 ci_test/test_dq_task.py
python3 ci_test/test_dq_task_wiring.py
python3 ci_test/test_validate_dq_configs.py
```

- [ ] **Step 3: Прогнать полный локальный набор проверок**

```bash
python3 ci_test/test_script.py
python3 ci_test/test_sync_dbt_sources.py
python3 ci_test/test_sync_iceberg_maintenance.py
python3 ci_test/test_run_pyspark_migrations.py
python3 scripts/validate_ranking_upload_configs.py
python3 scripts/validate_dq_configs.py
python3 ci_test/test_dq_config.py
python3 ci_test/test_dq_sql.py
python3 ci_test/test_dq_runner.py
python3 ci_test/test_dq_report.py
python3 ci_test/test_dq_results.py
python3 ci_test/test_dq_task.py
python3 ci_test/test_dq_task_wiring.py
python3 ci_test/test_validate_dq_configs.py
git diff --check
```
Expected: все команды завершаются успешно, `git diff --check` молчит.

- [ ] **Step 4: Коммит**

```bash
git add dq/README.md AGENTS.md
git commit -m "docs(dq): DQ package contract and handbook updates"
```

---

## Что остаётся за рамками этого плана

Отдельные планы после того, как пилот отработает неделю:

- **Фаза 2** — таска `dq` во все остальные энтити `layers/**` и `datasets/**`.
- **Фаза 3** — переключение 14 сенсоров и `upload/*/config/factory.py` на `external_task_id="dq"`.
- **Фаза 4** — отключение генерации `tests:`/`freshness:` в `scripts/sync_dbt_sources.py`, согласующий PR в `dbt-trino`, перевод двух DQ-панелей Grafana на `silver.airflow_task_instance`, дашборды Superset.
