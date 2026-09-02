# Feature Stats Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Для каждой из 7 репозиторно-управляемых таблиц, выгружаемых в сервис инференса, на каждой записанной партиции считать профиль распределения каждого числового признака и складывать его в Iceberg-таблицу.

**Architecture:** Новый top-level пакет `feature_stats/` — структурное зеркало `dq/`. Таска `feature_stats` собирается фабрикой и вешается в DAG энтити параллельно таске `dq`. Расчёт — один Trino-запрос на таблицу через `TrinoHook`: `count`, `avg`, `min`, `max` и `approx_percentile` по каждой числовой колонке за один скан партиции. Результат пишется в `iceberg.silver.feature_platform_feature_stats` через pyiceberg с идемпотентной перезаписью партиции. Хелперы адресации таблицы и предиката партиции переиспользуются из `dq.tests`, поэтому статистика физически не может уехать на другую партицию, чем DQ.

**Tech Stack:** Python 3.11, Trino SQL, PyIceberg, PyArrow, Airflow 3 TaskFlow API, PyYAML.

**Spec:** `docs/superpowers/specs/2026-08-23-feature-stats-design.md`

## Global Constraints

- Тесты в этом репозитории — **не pytest**. Это самостоятельные скрипты в `ci_test/`, где функции `test_*` вызываются вручную из `main()`, а файл заканчивается `raise SystemExit(main())`. Запуск: `python3 ci_test/test_x.py`. Ассерты — голый `assert`.
- Каждый тест-файл начинается с `sys.path.insert(0, str(Path(__file__).resolve().parents[1]))` — иначе пакет не импортируется.
- Весь SQL — **диалект Trino**, не Postgres. Предикатов `IS [NOT] TRUE` в Trino нет.
- Любая таблица в SQL адресуется полным именем `"<trino-catalog>".<schema>.<table>`, включая `information_schema`. Дефолтный каталог соединений `trino_*` — `hive`, неквалифицированное имя падает с `CATALOG_NOT_FOUND`.
- Литералы времени всегда с зоной: `TIMESTAMP '2026-08-22 06:00:00 UTC'`. Сессия Trino живёт в `Europe/Moscow`, голый `TIMESTAMP '...'` молча указывает на соседний снапшот.
- Набор перцентилей зашит в код и не конфигурируется: `(0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95)` → колонки `p05 p10 p25 p50 p75 p90 p95`.
- PyIceberg-идентификатор таблицы — ровно двухэлементный кортеж `(schema, name)`, построенный напрямую из `config.yaml`. Никаких `split`/`rsplit`/склейки строк.
- Новые `create_table.sql` обязаны содержать `CREATE TABLE IF NOT EXISTS` и `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')`.
- Комментарии и docstring'и — на русском, как во всём репозитории. Комментарий объясняет «почему», а не «что».
- Не запускай `cd` в путь репозитория: он содержит юникод в NFD-нормализации, и `cd` уводит в параллельное дерево, которого не видит git. Работай из дефолтного рабочего каталога по относительным путям.

---

### Task 1: Пакет `feature_stats` и разбор блока конфига

**Files:**
- Create: `feature_stats/__init__.py` (пустой)
- Create: `feature_stats/config.py`
- Test: `ci_test/test_feature_stats_config.py`

**Interfaces:**
- Consumes: `dq.config.RenderContext` (для поля `StatsContext.render`).
- Produces:
  - `PERCENTILES: tuple[float, ...]`, `PERCENTILE_COLUMNS: tuple[str, ...]`, `NUMERIC_TYPES: frozenset[str]`, `HOURS_PER_DAY: int`, `DEFAULT_TEAM: str`, `DEFAULT_PARTITION_DATE_TEMPLATE: str`
  - `class FeatureStatsConfigError(ValueError)`
  - `@dataclass(frozen=True) FeatureStatsSettings(enabled, trino_conn_id, partition_column, partition_date_template, partition_granularity, snapshot_interval_hours, exclude_columns, columns_per_query, query_timeout_seconds)`
  - `@dataclass(frozen=True) StatsContext(render: RenderContext, partition_ts: datetime)`
  - `load_feature_stats_settings(config: dict) -> FeatureStatsSettings`

- [ ] **Step 1: Создай пустой `feature_stats/__init__.py`**

```bash
mkdir -p feature_stats && : > feature_stats/__init__.py
```

- [ ] **Step 2: Напиши падающий тест `ci_test/test_feature_stats_config.py`**

```python
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import (
    PERCENTILE_COLUMNS,
    PERCENTILES,
    FeatureStatsConfigError,
    StatsContext,
    load_feature_stats_settings,
)

TABLE = {
    "catalog": "iceberg",
    "schema": "gold",
    "name": "feature_platform_sku_group_price_features",
    "primary_key": "date,sku_group_id",
}


def test_defaults_when_block_is_absent() -> None:
    settings = load_feature_stats_settings({"table": TABLE})
    assert settings.enabled is True
    assert settings.trino_conn_id == "trino_search"
    assert settings.partition_column == "date"
    assert settings.partition_granularity == "date"
    assert settings.snapshot_interval_hours == 24
    assert settings.exclude_columns == ()
    assert settings.columns_per_query is None
    assert settings.query_timeout_seconds == 600


def test_percentile_sets_stay_aligned() -> None:
    # Колонки p05..p95 в таблице результатов позиционно соответствуют PERCENTILES.
    assert PERCENTILES == (0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95)
    assert PERCENTILE_COLUMNS == ("p05", "p10", "p25", "p50", "p75", "p90", "p95")
    assert len(PERCENTILES) == len(PERCENTILE_COLUMNS)


def test_exclude_columns_are_normalized_to_a_tuple() -> None:
    settings = load_feature_stats_settings(
        {"table": TABLE, "feature_stats": {"exclude_columns": ["category_id", " brand_id "]}}
    )
    assert settings.exclude_columns == ("category_id", "brand_id")


def test_primary_key_is_required() -> None:
    try:
        load_feature_stats_settings({"table": {**TABLE, "primary_key": ""}})
    except FeatureStatsConfigError as error:
        assert "primary_key" in str(error)
    else:
        raise AssertionError("пустой primary_key обязан падать")


def test_unknown_key_is_rejected() -> None:
    try:
        load_feature_stats_settings({"table": TABLE, "feature_stats": {"percentiles": [0.5]}})
    except FeatureStatsConfigError as error:
        assert "percentiles" in str(error)
    else:
        raise AssertionError("неизвестный ключ обязан падать: набор перцентилей не конфигурируется")


def test_snapshot_requires_interval_and_template() -> None:
    base = {"partition_granularity": "timestamp", "partition_column": "calculated_at"}
    try:
        load_feature_stats_settings({"table": TABLE, "feature_stats": dict(base)})
    except FeatureStatsConfigError as error:
        assert "snapshot_interval_hours" in str(error)
    else:
        raise AssertionError("timestamp без snapshot_interval_hours обязан падать")

    try:
        load_feature_stats_settings(
            {"table": TABLE, "feature_stats": {**base, "snapshot_interval_hours": 3}}
        )
    except FeatureStatsConfigError as error:
        assert "partition_date_template" in str(error)
    else:
        raise AssertionError("timestamp без partition_date_template обязан падать")


def test_snapshot_block_is_accepted() -> None:
    settings = load_feature_stats_settings(
        {
            "table": TABLE,
            "feature_stats": {
                "partition_granularity": "timestamp",
                "partition_column": "calculated_at",
                "snapshot_interval_hours": 3,
                "partition_date_template": "{{ x }}",
            },
        }
    )
    assert settings.partition_granularity == "timestamp"
    assert settings.snapshot_interval_hours == 3
    assert settings.partition_column == "calculated_at"


def test_unknown_granularity_is_rejected() -> None:
    try:
        load_feature_stats_settings(
            {"table": TABLE, "feature_stats": {"partition_granularity": "hour"}}
        )
    except FeatureStatsConfigError as error:
        assert "partition_granularity" in str(error)
    else:
        raise AssertionError("неизвестная гранулярность обязана падать")


def test_columns_per_query_must_be_positive() -> None:
    try:
        load_feature_stats_settings({"table": TABLE, "feature_stats": {"columns_per_query": 0}})
    except FeatureStatsConfigError as error:
        assert "columns_per_query" in str(error)
    else:
        raise AssertionError("нулевой батч обязан падать: пустой список колонок не запрос")


def test_stats_context_holds_render_context_and_partition_ts() -> None:
    render = RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_sku_group_price_features",
        primary_key=("date", "sku_group_id"),
        partition_column="date",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
    )
    ctx = StatsContext(render=render, partition_ts=datetime(2026, 8, 22, tzinfo=timezone.utc))
    assert ctx.render.table == "feature_platform_sku_group_price_features"
    assert ctx.partition_ts.tzinfo is timezone.utc


def main() -> int:
    test_defaults_when_block_is_absent()
    test_percentile_sets_stay_aligned()
    test_exclude_columns_are_normalized_to_a_tuple()
    test_primary_key_is_required()
    test_unknown_key_is_rejected()
    test_snapshot_requires_interval_and_template()
    test_snapshot_block_is_accepted()
    test_unknown_granularity_is_rejected()
    test_columns_per_query_must_be_positive()
    test_stats_context_holds_render_context_and_partition_ts()
    print("Feature stats config tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3: Убедись, что тест падает**

Run: `python3 ci_test/test_feature_stats_config.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'feature_stats.config'`

- [ ] **Step 4: Напиши `feature_stats/config.py`**

```python
"""Чтение и валидация блока feature_stats: из config.yaml энтити."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dq.config import RenderContext

HOURS_PER_DAY = 24
PARTITION_GRANULARITIES = ("date", "timestamp")
DEFAULT_TEAM = "team:search"
DEFAULT_PARTITION_DATE_TEMPLATE = "{{ macros.ds_add(ds, -1) }}"
DEFAULT_QUERY_TIMEOUT_SECONDS = 600

# Набор перцентилей зашит в код: таблица результатов держит их в отдельных колонках
# p05..p95, и конфигурируемый набор потребовал бы long-формата и миграции на каждое
# изменение. PERCENTILES и PERCENTILE_COLUMNS соответствуют позиционно.
PERCENTILES: tuple[float, ...] = (0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95)
PERCENTILE_COLUMNS: tuple[str, ...] = ("p05", "p10", "p25", "p50", "p75", "p90", "p95")

# Числовые типы Trino. Проверка идёт точным совпадением, а не префиксом: "interval day
# to second" начинается с "int" и префиксной проверкой попал бы в признаки.
NUMERIC_TYPES = frozenset({"tinyint", "smallint", "integer", "bigint", "real", "double"})

KNOWN_KEYS = frozenset(
    {
        "enabled",
        "trino_conn_id",
        "partition_column",
        "partition_date_template",
        "partition_granularity",
        "snapshot_interval_hours",
        "exclude_columns",
        "columns_per_query",
        "query_timeout_seconds",
    }
)


class FeatureStatsConfigError(ValueError):
    """Некорректный блок feature_stats: в config.yaml."""


@dataclass(frozen=True)
class FeatureStatsSettings:
    enabled: bool
    trino_conn_id: str
    partition_column: str
    partition_date_template: str
    partition_granularity: str
    snapshot_interval_hours: int
    exclude_columns: tuple[str, ...]
    columns_per_query: int | None
    query_timeout_seconds: int


@dataclass(frozen=True)
class StatsContext:
    """Что и за какую партицию считаем.

    `render` — контекст DQ: он даёт и адрес таблицы, и предикат партиции, поэтому
    статистика физически не может уехать на другую партицию, чем проверял DQ.
    `partition_ts` заполнен всегда: у снапшотной энтити это записанный снапшот,
    у дневной — полночь UTC её партиции. Он же — часть ключа таблицы результатов
    и фильтра идемпотентной перезаписи.
    """

    render: RenderContext
    partition_ts: datetime


def load_feature_stats_settings(config: dict[str, Any]) -> FeatureStatsSettings:
    table = config.get("table") or {}
    primary_key = tuple(
        column.strip() for column in str(table.get("primary_key", "")).split(",") if column.strip()
    )
    if not primary_key:
        raise FeatureStatsConfigError("table.primary_key обязателен для feature_stats")

    raw = config.get("feature_stats") or {}
    if not isinstance(raw, dict):
        raise FeatureStatsConfigError("Блок feature_stats: должен быть отображением")

    unknown = set(raw) - KNOWN_KEYS
    if unknown:
        raise FeatureStatsConfigError(
            f"feature_stats: неизвестные параметры {sorted(unknown)}. "
            f"Доступные: {', '.join(sorted(KNOWN_KEYS))}"
        )

    granularity = str(raw.get("partition_granularity", "date"))
    if granularity not in PARTITION_GRANULARITIES:
        raise FeatureStatsConfigError(
            f"feature_stats.partition_granularity должен быть одним из {PARTITION_GRANULARITIES}, "
            f"получено {granularity!r}"
        )

    snapshot_interval_hours = HOURS_PER_DAY
    if granularity == "timestamp":
        if "snapshot_interval_hours" not in raw:
            raise FeatureStatsConfigError(
                "feature_stats.snapshot_interval_hours обязателен при partition_granularity: "
                "timestamp — он должен повторять шаг расписания DAG'а"
            )
        snapshot_interval_hours = int(raw["snapshot_interval_hours"])
        if snapshot_interval_hours <= 0:
            raise FeatureStatsConfigError(
                "feature_stats.snapshot_interval_hours должен быть положительным"
            )
        if "partition_date_template" not in raw:
            raise FeatureStatsConfigError(
                "feature_stats.partition_date_template обязателен при partition_granularity: "
                "timestamp — дефолтный шаблон отдаёт дату, а снапшоту нужен полный timestamp"
            )

    columns_per_query = raw.get("columns_per_query")
    if columns_per_query is not None:
        columns_per_query = int(columns_per_query)
        if columns_per_query <= 0:
            raise FeatureStatsConfigError(
                "feature_stats.columns_per_query должен быть положительным или null; "
                "null означает один запрос на всю таблицу"
            )

    exclude_raw = raw.get("exclude_columns") or []
    if not isinstance(exclude_raw, list):
        raise FeatureStatsConfigError("feature_stats.exclude_columns должен быть списком")

    return FeatureStatsSettings(
        enabled=_parse_bool(raw.get("enabled", True), "feature_stats.enabled"),
        trino_conn_id=str(raw.get("trino_conn_id", "trino_search")),
        partition_column=str(raw.get("partition_column", "date")),
        partition_date_template=str(
            raw.get("partition_date_template", DEFAULT_PARTITION_DATE_TEMPLATE)
        ),
        partition_granularity=granularity,
        snapshot_interval_hours=snapshot_interval_hours,
        exclude_columns=tuple(str(column).strip() for column in exclude_raw if str(column).strip()),
        columns_per_query=columns_per_query,
        query_timeout_seconds=int(
            raw.get("query_timeout_seconds", DEFAULT_QUERY_TIMEOUT_SECONDS)
        ),
    )


def _parse_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().strip('"').strip("'").lower()
    if normalized in {"true", "yes", "1"}:
        return True
    if normalized in {"false", "no", "0"}:
        return False
    raise FeatureStatsConfigError(f"{field_name} должен быть булевым, получено {value!r}")
```

- [ ] **Step 5: Убедись, что тест проходит**

Run: `python3 ci_test/test_feature_stats_config.py`
Expected: PASS — `Feature stats config tests completed successfully`

- [ ] **Step 6: Коммит**

```bash
git add feature_stats/__init__.py feature_stats/config.py ci_test/test_feature_stats_config.py
git commit -m "feat(feature_stats): блок конфига и контекст расчёта"
```

---

### Task 2: Рендер Trino SQL

**Files:**
- Create: `feature_stats/query.py`
- Test: `ci_test/test_feature_stats_sql.py`

**Interfaces:**
- Consumes: `feature_stats.config.{PERCENTILES, StatsContext}`; `dq.tests.{quote_identifier, quote_literal, table_ref, partition_expression, partition_literal}`.
- Produces:
  - `VALUES_PER_COLUMN: int` (= 5)
  - `percentile_array_literal() -> str`
  - `render_columns_query(ctx: StatsContext) -> str`
  - `render_stats_query(ctx: StatsContext, columns: Sequence[str]) -> str`

- [ ] **Step 1: Напиши падающий тест `ci_test/test_feature_stats_sql.py`**

```python
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import PERCENTILES, StatsContext
from feature_stats.query import (
    VALUES_PER_COLUMN,
    percentile_array_literal,
    render_columns_query,
    render_stats_query,
)

DAILY = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_sku_group_price_features",
        primary_key=("date", "sku_group_id"),
        partition_column="date",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
    ),
    partition_ts=datetime(2026, 8, 22, tzinfo=timezone.utc),
)

SNAPSHOT = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_dynamic_pricing_sku_group_price_features",
        primary_key=("calculated_at", "sku_group_id", "promotion_id"),
        partition_column="calculated_at",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
        partition_granularity="timestamp",
        partition_timestamp=datetime(2026, 8, 22, 6, 0, 0),
        snapshot_interval_hours=3,
    ),
    partition_ts=datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc),
)


def test_values_per_column_matches_the_select_list() -> None:
    # cnt, mean, min, max, pct — раннер режет строку результата по этому шагу.
    assert VALUES_PER_COLUMN == 5


def test_percentile_array_literal_matches_the_configured_set() -> None:
    assert percentile_array_literal() == "ARRAY[0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]"
    assert len(PERCENTILES) == 7


def test_columns_query_qualifies_information_schema_with_the_catalog() -> None:
    sql = render_columns_query(DAILY)
    # Дефолтный каталог соединений trino_* — hive; без квалификации это CATALOG_NOT_FOUND.
    assert '"dwh-iceberg".information_schema.columns' in sql
    assert "table_schema = 'gold'" in sql
    assert "table_name = 'feature_platform_sku_group_price_features'" in sql
    assert "ORDER BY ordinal_position" in sql


def test_stats_query_daily_partition() -> None:
    sql = render_stats_query(DAILY, ["sell_price_eod", "abs_discount"])
    assert '"dwh-iceberg"."gold"."feature_platform_sku_group_price_features"' in sql
    assert "count(*) AS rows_total" in sql
    assert 'count("sell_price_eod") AS cnt_0' in sql
    assert 'avg(CAST("sell_price_eod" AS DOUBLE)) AS mean_0' in sql
    assert 'min(CAST("sell_price_eod" AS DOUBLE)) AS min_0' in sql
    assert 'max(CAST("sell_price_eod" AS DOUBLE)) AS max_0' in sql
    assert (
        'approx_percentile(CAST("sell_price_eod" AS DOUBLE), '
        "ARRAY[0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]) AS pct_0"
    ) in sql
    assert 'count("abs_discount") AS cnt_1' in sql
    assert "CAST(\"date\" AS DATE) = DATE '2026-08-22'" in sql


def test_stats_query_aliases_are_positional_not_derived_from_names() -> None:
    # Имя признака может превысить лимит идентификатора или совпасть после нормализации.
    sql = render_stats_query(DAILY, ["a", "b", "c"])
    for index in range(3):
        assert f"AS cnt_{index}" in sql
    assert "AS cnt_a" not in sql


def test_stats_query_snapshot_pins_the_utc_instant() -> None:
    sql = render_stats_query(SNAPSHOT, ["avg_sell_price"])
    # Сессия Trino в Europe/Moscow: голый TIMESTAMP молча указал бы на соседний снапшот.
    assert "\"calculated_at\" = TIMESTAMP '2026-08-22 06:00:00 UTC'" in sql
    assert "CAST(" not in sql.split("WHERE")[1]


def test_stats_query_rejects_an_empty_column_list() -> None:
    try:
        render_stats_query(DAILY, [])
    except ValueError as error:
        assert "колонок" in str(error)
    else:
        raise AssertionError("пустой список колонок обязан падать, а не рендерить голый count(*)")


def main() -> int:
    test_values_per_column_matches_the_select_list()
    test_percentile_array_literal_matches_the_configured_set()
    test_columns_query_qualifies_information_schema_with_the_catalog()
    test_stats_query_daily_partition()
    test_stats_query_aliases_are_positional_not_derived_from_names()
    test_stats_query_snapshot_pins_the_utc_instant()
    test_stats_query_rejects_an_empty_column_list()
    print("Feature stats SQL tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Убедись, что тест падает**

Run: `python3 ci_test/test_feature_stats_sql.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'feature_stats.query'`

- [ ] **Step 3: Напиши `feature_stats/query.py`**

```python
"""Рендер Trino SQL для профилей распределения признаков.

Запрос статистик возвращает ровно одну строку следующей раскладки:
  [rows_total, cnt_0, mean_0, min_0, max_0, pct_0, cnt_1, mean_1, ...]
то есть одно общее значение плюс VALUES_PER_COLUMN значений на каждый признак
в том же порядке, в каком признаки переданы в render_stats_query.
"""

from __future__ import annotations

from typing import Sequence

from dq.tests import (
    partition_expression,
    partition_literal,
    quote_identifier,
    quote_literal,
    table_ref,
)

from feature_stats.config import PERCENTILES, StatsContext

# cnt, mean, min, max, pct
VALUES_PER_COLUMN = 5


def percentile_array_literal() -> str:
    return "ARRAY[" + ", ".join(repr(value) for value in PERCENTILES) + "]"


def render_columns_query(ctx: StatsContext) -> str:
    """SQL, отдающий имена и типы колонок целевой таблицы в порядке объявления."""
    # information_schema резолвится в дефолтном каталоге соединения, а он у trino_*
    # смотрит в hive. Квалифицируем каталогом таблицы, иначе CATALOG_NOT_FOUND.
    return (
        f"SELECT column_name, data_type\n"
        f"FROM {quote_identifier(ctx.render.catalog_alias)}.information_schema.columns\n"
        f"WHERE table_schema = {quote_literal(ctx.render.schema)}\n"
        f"  AND table_name = {quote_literal(ctx.render.table)}\n"
        f"ORDER BY ordinal_position"
    )


def render_stats_query(ctx: StatsContext, columns: Sequence[str]) -> str:
    """SQL профиля распределения для одной партии признаков."""
    if not columns:
        raise ValueError("render_stats_query вызван без колонок")

    percentiles = percentile_array_literal()
    projections = ["  count(*) AS rows_total"]
    for index, column in enumerate(columns):
        quoted = quote_identifier(column)
        # Алиасы позиционные: имя признака может превысить лимит идентификатора
        # или совпасть с соседним после нормализации.
        value = f"CAST({quoted} AS DOUBLE)"
        projections.extend(
            [
                f"  count({quoted}) AS cnt_{index}",
                f"  avg({value}) AS mean_{index}",
                f"  min({value}) AS min_{index}",
                f"  max({value}) AS max_{index}",
                f"  approx_percentile({value}, {percentiles}) AS pct_{index}",
            ]
        )

    return (
        "SELECT\n"
        + ",\n".join(projections)
        + f"\nFROM {table_ref(ctx.render)}\n"
        + f"WHERE {partition_expression(ctx.render)} = {partition_literal(ctx.render)}"
    )
```

- [ ] **Step 4: Убедись, что тест проходит**

Run: `python3 ci_test/test_feature_stats_sql.py`
Expected: PASS — `Feature stats SQL tests completed successfully`

- [ ] **Step 5: Коммит**

```bash
git add feature_stats/query.py ci_test/test_feature_stats_sql.py
git commit -m "feat(feature_stats): рендер Trino SQL профилей признаков"
```

---

### Task 3: Определение набора признаков и разбор результата

**Files:**
- Create: `feature_stats/runner.py`
- Test: `ci_test/test_feature_stats_runner.py`

**Interfaces:**
- Consumes: `feature_stats.config.{NUMERIC_TYPES, PERCENTILE_COLUMNS, FeatureStatsSettings, StatsContext, load_feature_stats_settings}`; `feature_stats.query.{VALUES_PER_COLUMN, render_columns_query, render_stats_query}`.
- Produces:
  - `class FeatureStatsError(RuntimeError)`
  - `@dataclass(frozen=True) FeatureStat(feature_name, data_type, rows_total, non_null_count, null_share, mean, min_value, max_value, percentiles, duration_ms, sql)`
  - `is_numeric(data_type: str) -> bool`
  - `fetch_typed_columns(query, ctx) -> list[tuple[str, str]]`
  - `select_feature_columns(typed_columns, settings, ctx) -> list[tuple[str, str]]`
  - `batches(columns, columns_per_query) -> list[list[tuple[str, str]]]`
  - `parse_stats_row(row, batch, duration_ms, sql) -> list[FeatureStat]`
  - `run_feature_stats(settings, ctx, query) -> list[FeatureStat]`

- [ ] **Step 1: Напиши падающий тест `ci_test/test_feature_stats_runner.py`**

```python
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import StatsContext, load_feature_stats_settings
from feature_stats.runner import (
    FeatureStatsError,
    batches,
    is_numeric,
    parse_stats_row,
    run_feature_stats,
    select_feature_columns,
)

TABLE = {
    "catalog": "iceberg",
    "schema": "gold",
    "name": "feature_platform_sku_group_search_conversion_features_v2",
    "primary_key": "date,sku_group_id",
}

CTX = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_sku_group_search_conversion_features_v2",
        primary_key=("date", "sku_group_id"),
        partition_column="date",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
    ),
    partition_ts=datetime(2026, 8, 22, tzinfo=timezone.utc),
)

TYPED_COLUMNS = [
    ("date", "date"),
    ("sku_group_id", "bigint"),
    ("category_id", "bigint"),
    ("conv_imp2order_7", "double"),
    ("skg_days_since_last_atc", "integer"),
    ("price_bucket", "decimal(18,2)"),
    ("query", "varchar"),
    ("window_length", "interval day to second"),
]


def settings(**block):
    return load_feature_stats_settings({"table": TABLE, "feature_stats": block})


def test_is_numeric_accepts_trino_numeric_types() -> None:
    for data_type in ("tinyint", "smallint", "integer", "bigint", "real", "double", "decimal(18,2)"):
        assert is_numeric(data_type), data_type


def test_is_numeric_rejects_interval_despite_the_int_prefix() -> None:
    # Префиксная проверка утащила бы "interval day to second" в признаки.
    assert not is_numeric("interval day to second")
    assert not is_numeric("varchar")
    assert not is_numeric("date")
    assert not is_numeric("timestamp(6) with time zone")


def test_select_drops_keys_partition_and_excluded() -> None:
    selected = select_feature_columns(TYPED_COLUMNS, settings(exclude_columns=["category_id"]), CTX)
    assert [name for name, _ in selected] == [
        "conv_imp2order_7",
        "skg_days_since_last_atc",
        "price_bucket",
    ]


def test_select_preserves_declaration_order() -> None:
    selected = select_feature_columns(TYPED_COLUMNS, settings(), CTX)
    assert [name for name, _ in selected][:2] == ["category_id", "conv_imp2order_7"]


def test_select_rejects_an_exclude_column_that_does_not_exist() -> None:
    # Опечатка в exclude_columns иначе молча вернула бы признак под наблюдение.
    try:
        select_feature_columns(TYPED_COLUMNS, settings(exclude_columns=["categoryy_id"]), CTX)
    except FeatureStatsError as error:
        assert "categoryy_id" in str(error)
    else:
        raise AssertionError("несуществующая колонка в exclude_columns обязана падать")


def test_batches_default_to_a_single_query() -> None:
    columns = [(f"c{i}", "double") for i in range(5)]
    assert batches(columns, None) == [columns]


def test_batches_split_by_size() -> None:
    columns = [(f"c{i}", "double") for i in range(5)]
    split = batches(columns, 2)
    assert [len(chunk) for chunk in split] == [2, 2, 1]


def test_parse_stats_row_maps_every_metric() -> None:
    batch = [("conv_imp2order_7", "double"), ("skg_return_rate_7", "double")]
    row = [
        1000,
        800, 0.25, 0.0, 1.0, [0.01, 0.02, 0.1, 0.2, 0.4, 0.7, 0.9],
        1000, 0.5, 0.1, 0.9, [0.11, 0.12, 0.2, 0.5, 0.7, 0.8, 0.85],
    ]
    stats = parse_stats_row(row, batch, 1500, "SELECT 1")
    assert [stat.feature_name for stat in stats] == ["conv_imp2order_7", "skg_return_rate_7"]
    first = stats[0]
    assert first.rows_total == 1000
    assert first.non_null_count == 800
    assert abs(first.null_share - 0.2) < 1e-9
    assert first.mean == 0.25
    assert first.min_value == 0.0
    assert first.max_value == 1.0
    assert first.percentiles == (0.01, 0.02, 0.1, 0.2, 0.4, 0.7, 0.9)
    assert first.duration_ms == 1500
    assert first.sql == "SELECT 1"
    assert stats[1].null_share == 0.0


def test_parse_stats_row_handles_a_fully_null_feature() -> None:
    # approx_percentile отдаёт NULL вместо массива, но строка всё равно нужна:
    # "признак целиком пустой на этой партиции" — сам по себе сигнал.
    batch = [("ratio_crnt_min_to_avg_min_full_price_30", "double")]
    stats = parse_stats_row([1000, 0, None, None, None, None], batch, 10, "SELECT 1")
    assert len(stats) == 1
    assert stats[0].non_null_count == 0
    assert stats[0].null_share == 1.0
    assert stats[0].mean is None
    assert stats[0].percentiles == (None,) * 7


def test_parse_stats_row_handles_an_empty_partition() -> None:
    batch = [("conv_imp2order_7", "double")]
    stats = parse_stats_row([0, 0, None, None, None, None], batch, 10, "SELECT 1")
    assert stats[0].rows_total == 0
    assert stats[0].null_share is None


def test_parse_stats_row_rejects_a_percentile_array_of_wrong_length() -> None:
    batch = [("conv_imp2order_7", "double")]
    try:
        parse_stats_row([10, 10, 0.5, 0.0, 1.0, [0.1, 0.2]], batch, 10, "SELECT 1")
    except FeatureStatsError as error:
        assert "перцентил" in str(error)
    else:
        raise AssertionError("несовпадение длины массива перцентилей обязано падать")


def test_run_feature_stats_end_to_end_on_a_fake_query() -> None:
    seen = []

    def query(sql: str):
        seen.append(sql)
        if "information_schema" in sql:
            return TYPED_COLUMNS
        # 1 + 3 признака * 5 значений = 16 позиций
        return [
            [1000, 900, 0.3, 0.0, 1.0, [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]]
            + [900, 2.0, 1.0, 3.0, [1.0, 1.1, 1.2, 2.0, 2.5, 2.8, 2.9]]
            + [1000, 5.0, 0.0, 9.0, [0.5, 1.0, 2.0, 5.0, 7.0, 8.0, 8.5]]
        ]

    stats = run_feature_stats(settings(exclude_columns=["category_id"]), CTX, query)
    assert len(seen) == 2
    assert "information_schema" in seen[0]
    assert [stat.feature_name for stat in stats] == [
        "conv_imp2order_7",
        "skg_days_since_last_atc",
        "price_bucket",
    ]


def test_run_feature_stats_returns_nothing_when_disabled() -> None:
    def query(sql: str):
        raise AssertionError("выключенная таска не должна ходить в Trino")

    assert run_feature_stats(settings(enabled=False), CTX, query) == []


def test_run_feature_stats_fails_when_the_table_is_missing() -> None:
    def query(sql: str):
        return []

    try:
        run_feature_stats(settings(), CTX, query)
    except FeatureStatsError as error:
        assert "feature_platform_sku_group_search_conversion_features_v2" in str(error)
    else:
        raise AssertionError("отсутствующая таблица обязана падать диагностикой")


def test_run_feature_stats_is_a_noop_on_a_key_only_table() -> None:
    def query(sql: str):
        if "information_schema" in sql:
            return [("date", "date"), ("sku_group_id", "bigint")]
        raise AssertionError("без признаков запрос статистик не нужен")

    assert run_feature_stats(settings(), CTX, query) == []


def main() -> int:
    test_is_numeric_accepts_trino_numeric_types()
    test_is_numeric_rejects_interval_despite_the_int_prefix()
    test_select_drops_keys_partition_and_excluded()
    test_select_preserves_declaration_order()
    test_select_rejects_an_exclude_column_that_does_not_exist()
    test_batches_default_to_a_single_query()
    test_batches_split_by_size()
    test_parse_stats_row_maps_every_metric()
    test_parse_stats_row_handles_a_fully_null_feature()
    test_parse_stats_row_handles_an_empty_partition()
    test_parse_stats_row_rejects_a_percentile_array_of_wrong_length()
    test_run_feature_stats_end_to_end_on_a_fake_query()
    test_run_feature_stats_returns_nothing_when_disabled()
    test_run_feature_stats_fails_when_the_table_is_missing()
    test_run_feature_stats_is_a_noop_on_a_key_only_table()
    print("Feature stats runner tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Убедись, что тест падает**

Run: `python3 ci_test/test_feature_stats_runner.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'feature_stats.runner'`

- [ ] **Step 3: Напиши `feature_stats/runner.py`**

```python
"""Определение набора признаков и расчёт их профилей распределения."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from feature_stats.config import (
    NUMERIC_TYPES,
    PERCENTILE_COLUMNS,
    FeatureStatsSettings,
    StatsContext,
)
from feature_stats.query import VALUES_PER_COLUMN, render_columns_query, render_stats_query

Query = Callable[[str], list]
TypedColumn = tuple[str, str]


class FeatureStatsError(RuntimeError):
    """Расчёт невозможен: таблица недоступна, конфиг врёт или Trino вернул не то."""


@dataclass(frozen=True)
class FeatureStat:
    feature_name: str
    data_type: str
    rows_total: int
    non_null_count: int
    null_share: float | None
    mean: float | None
    min_value: float | None
    max_value: float | None
    percentiles: tuple[float | None, ...]
    duration_ms: int
    sql: str


def is_numeric(data_type: str) -> bool:
    normalized = str(data_type).strip().lower()
    return normalized in NUMERIC_TYPES or normalized.startswith("decimal(")


def fetch_typed_columns(query: Query, ctx: StatsContext) -> list[TypedColumn]:
    rows = query(render_columns_query(ctx))
    if not rows:
        raise FeatureStatsError(
            f"Таблица {ctx.render.catalog_alias}.{ctx.render.schema}.{ctx.render.table} "
            "не найдена в каталоге Trino. Таска feature_stats запускается только после того, "
            "как миграции применены; проверьте, что миграция энтити проехала на master."
        )
    return [(str(row[0]), str(row[1])) for row in rows]


def select_feature_columns(
    typed_columns: Sequence[TypedColumn], settings: FeatureStatsSettings, ctx: StatsContext
) -> list[TypedColumn]:
    """Числовые колонки-признаки в порядке объявления в таблице."""
    available = {name for name, _ in typed_columns}
    missing = [column for column in settings.exclude_columns if column not in available]
    if missing:
        raise FeatureStatsError(
            f"feature_stats.exclude_columns ссылается на колонки {missing}, которых нет в "
            f"{ctx.render.catalog_alias}.{ctx.render.schema}.{ctx.render.table}. "
            "Опечатка молча вернула бы признак под наблюдение, поэтому это ошибка конфигурации."
        )

    skip = set(ctx.render.primary_key) | {settings.partition_column} | set(settings.exclude_columns)
    return [
        (name, data_type)
        for name, data_type in typed_columns
        if name not in skip and is_numeric(data_type)
    ]


def batches(
    columns: Sequence[TypedColumn], columns_per_query: int | None
) -> list[list[TypedColumn]]:
    """Разбиение на партии запросов.

    Дефолт — одна партия: каждый дополнительный запрос это ещё один полный скан
    партиции, что дороже широкого списка агрегатов в одном плане.
    """
    if not columns_per_query:
        return [list(columns)]
    return [
        list(columns[start : start + columns_per_query])
        for start in range(0, len(columns), columns_per_query)
    ]


def parse_stats_row(
    row: Sequence[Any], batch: Sequence[TypedColumn], duration_ms: int, sql: str
) -> list[FeatureStat]:
    expected_width = 1 + len(batch) * VALUES_PER_COLUMN
    if len(row) != expected_width:
        raise FeatureStatsError(
            f"Trino вернул {len(row)} значений вместо {expected_width} "
            f"для {len(batch)} признаков — раскладка строки результата разъехалась с рендером"
        )

    rows_total = int(row[0]) if row[0] is not None else 0
    stats: list[FeatureStat] = []
    for index, (name, data_type) in enumerate(batch):
        base = 1 + index * VALUES_PER_COLUMN
        non_null_count = int(row[base]) if row[base] is not None else 0
        stats.append(
            FeatureStat(
                feature_name=name,
                data_type=data_type,
                rows_total=rows_total,
                non_null_count=non_null_count,
                null_share=(1.0 - non_null_count / rows_total) if rows_total else None,
                mean=_as_float(row[base + 1]),
                min_value=_as_float(row[base + 2]),
                max_value=_as_float(row[base + 3]),
                percentiles=_as_percentiles(row[base + 4], name),
                duration_ms=duration_ms,
                sql=sql,
            )
        )
    return stats


def run_feature_stats(
    settings: FeatureStatsSettings, ctx: StatsContext, query: Query
) -> list[FeatureStat]:
    if not settings.enabled:
        return []

    features = select_feature_columns(fetch_typed_columns(query, ctx), settings, ctx)
    if not features:
        # Таблица из одних ключей — не ошибка, просто нечего профилировать.
        return []

    stats: list[FeatureStat] = []
    for batch in batches(features, settings.columns_per_query):
        sql = render_stats_query(ctx, [name for name, _ in batch])
        started = time.monotonic()
        rows = query(sql)
        duration_ms = int((time.monotonic() - started) * 1000)
        if not rows:
            raise FeatureStatsError(
                "Агрегатный запрос без GROUP BY обязан вернуть строку, получено пусто. "
                f"SQL:\n{sql}"
            )
        stats.extend(parse_stats_row(rows[0], batch, duration_ms, sql))
    return stats


def _as_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _as_percentiles(value: Any, feature_name: str) -> tuple[float | None, ...]:
    # При нуле непустых значений approx_percentile отдаёт NULL вместо массива.
    if value is None:
        return (None,) * len(PERCENTILE_COLUMNS)
    values = list(value)
    if len(values) != len(PERCENTILE_COLUMNS):
        raise FeatureStatsError(
            f"{feature_name}: Trino вернул {len(values)} перцентилей вместо "
            f"{len(PERCENTILE_COLUMNS)}"
        )
    return tuple(_as_float(item) for item in values)
```

- [ ] **Step 4: Убедись, что тест проходит**

Run: `python3 ci_test/test_feature_stats_runner.py`
Expected: PASS — `Feature stats runner tests completed successfully`

- [ ] **Step 5: Коммит**

```bash
git add feature_stats/runner.py ci_test/test_feature_stats_runner.py
git commit -m "feat(feature_stats): отбор признаков и разбор профилей"
```

---

### Task 4: Таблица результатов и запись в Iceberg

**Files:**
- Create: `feature_stats/results/config.yaml`
- Create: `feature_stats/results/migrations/create_table.sql`
- Create: `feature_stats/results_writer.py`
- Modify: `scripts/run_pyspark_migrations.py:134`
- Modify: `scripts/sync_iceberg_maintenance.py:16`
- Test: `ci_test/test_feature_stats_results.py`

**Interfaces:**
- Consumes: `feature_stats.config.{PERCENTILE_COLUMNS, StatsContext}`; `feature_stats.runner.FeatureStat`; `dq.results_writer.{catalog_properties, load_results_catalog}`.
- Produces:
  - `RESULTS_CONFIG_PATH: Path`
  - `@dataclass(frozen=True) RunMeta(dag_id, task_id, run_id, try_number, run_ts)`
  - `results_table_ref(repo_root: Path) -> tuple[str, str]`
  - `results_catalog_name(repo_root: Path) -> str`
  - `overwrite_filter_values(ctx, meta) -> dict[str, Any]`
  - `build_rows(stats, ctx, meta) -> list[dict]`
  - `write_results(repo_root, stats, ctx, meta) -> None`

- [ ] **Step 1: Создай конфиг и миграцию таблицы результатов**

`feature_stats/results/config.yaml`:

```yaml
# Таблица профилей распределения признаков. Пишется таской feature_stats каждого DAG'а.
# create_dbt_pr: false — таблица не уезжает в dbt-trino и не заводит DQ-тесты сама на себя.
table:
  key: feature_stats
  catalog: iceberg
  schema: silver
  name: feature_platform_feature_stats
  primary_key: date,partition_ts,dag_id,table_name,feature_name
  meta:
    team: team:search
    create_dbt_pr: false
    create_maintenance_pr: true
```

`feature_stats/results/migrations/create_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиции целевой таблицы; у снапшотной энтити — календарная дата снапшота',
    partition_ts TIMESTAMP COMMENT 'Момент партиции в UTC: сам снапшот у снапшотной энтити, полночь дня у дневной',
    run_ts TIMESTAMP COMMENT 'Момент прогона',
    dag_id STRING COMMENT 'DAG, внутри которого выполнялась таска',
    task_id STRING COMMENT 'Идентификатор таски, всегда feature_stats',
    run_id STRING COMMENT 'Airflow run_id прогона',
    try_number INT COMMENT 'Номер попытки таски',
    catalog STRING COMMENT 'Каталог целевой таблицы из config.yaml',
    schema_name STRING COMMENT 'Схема целевой таблицы',
    table_name STRING COMMENT 'Имя целевой таблицы',
    team STRING COMMENT 'Команда-владелец из table.meta.team, по умолчанию team:search',
    feature_name STRING COMMENT 'Имя колонки-признака',
    data_type STRING COMMENT 'Тип колонки в Trino на момент расчёта',
    rows_total BIGINT COMMENT 'Всего строк в партиции',
    non_null_count BIGINT COMMENT 'Строк с непустым значением признака',
    null_share DOUBLE COMMENT 'Доля NULL: 1 - non_null_count / rows_total',
    mean DOUBLE COMMENT 'Среднее по непустым значениям',
    min_value DOUBLE COMMENT 'Минимум по непустым значениям',
    max_value DOUBLE COMMENT 'Максимум по непустым значениям',
    p05 DOUBLE COMMENT 'approx_percentile 0.05',
    p10 DOUBLE COMMENT 'approx_percentile 0.1',
    p25 DOUBLE COMMENT 'approx_percentile 0.25',
    p50 DOUBLE COMMENT 'approx_percentile 0.5',
    p75 DOUBLE COMMENT 'approx_percentile 0.75',
    p90 DOUBLE COMMENT 'approx_percentile 0.9',
    p95 DOUBLE COMMENT 'approx_percentile 0.95',
    duration_ms BIGINT COMMENT 'Длительность запроса, в котором посчитан этот признак',
    sql_text STRING COMMENT 'Отрендеренный SQL расчёта'
)
USING iceberg
COMMENT 'Профили распределения признаков таблиц Feature Platform'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
```

- [ ] **Step 2: Напиши падающий тест `ci_test/test_feature_stats_results.py`**

```python
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import PERCENTILE_COLUMNS, StatsContext
from feature_stats.results_writer import (
    RunMeta,
    build_rows,
    overwrite_filter_values,
    results_catalog_name,
    results_table_ref,
)
from feature_stats.runner import FeatureStat

DAILY = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_sku_group_price_features",
        primary_key=("date", "sku_group_id"),
        partition_column="date",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
        team="team:search",
    ),
    partition_ts=datetime(2026, 8, 22, tzinfo=timezone.utc),
)

SNAPSHOT = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_dynamic_pricing_sku_group_price_features",
        primary_key=("calculated_at", "sku_group_id", "promotion_id"),
        partition_column="calculated_at",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
        team="team:recsys",
        partition_granularity="timestamp",
        partition_timestamp=datetime(2026, 8, 22, 6, 0, 0),
        snapshot_interval_hours=3,
    ),
    partition_ts=datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc),
)

META = RunMeta(
    dag_id="feature-platform.layers.gold.sku_group_id.sku_group_price_features",
    task_id="feature_stats",
    run_id="scheduled__2026-08-22T02:00:00+00:00",
    try_number=1,
    run_ts=datetime(2026, 8, 23, 2, 5, tzinfo=timezone.utc),
)

STAT = FeatureStat(
    feature_name="sell_price_eod",
    data_type="double",
    rows_total=5_400_000,
    non_null_count=5_400_000,
    null_share=0.0,
    mean=125.5,
    min_value=1.0,
    max_value=99000.0,
    percentiles=(5.0, 9.0, 30.0, 90.0, 190.0, 450.0, 800.0),
    duration_ms=8400,
    sql="SELECT 1",
)


def test_results_table_ref_comes_from_config() -> None:
    assert results_table_ref(Path(".")) == ("silver", "feature_platform_feature_stats")


def test_results_catalog_name_comes_from_config() -> None:
    assert results_catalog_name(Path(".")) == "iceberg"


def test_build_rows_maps_every_field() -> None:
    row = build_rows([STAT], DAILY, META)[0]
    assert row["date"] == date(2026, 8, 22)
    assert row["partition_ts"] == datetime(2026, 8, 22, tzinfo=timezone.utc)
    assert row["run_ts"] == META.run_ts
    assert row["dag_id"] == META.dag_id
    assert row["task_id"] == "feature_stats"
    assert row["run_id"] == META.run_id
    assert row["try_number"] == 1
    assert row["catalog"] == "dwh-iceberg"
    assert row["schema_name"] == "gold"
    assert row["table_name"] == "feature_platform_sku_group_price_features"
    assert row["team"] == "team:search"
    assert row["feature_name"] == "sell_price_eod"
    assert row["data_type"] == "double"
    assert row["rows_total"] == 5_400_000
    assert row["non_null_count"] == 5_400_000
    assert row["null_share"] == 0.0
    assert row["mean"] == 125.5
    assert row["min_value"] == 1.0
    assert row["max_value"] == 99000.0
    assert row["duration_ms"] == 8400
    assert row["sql_text"] == "SELECT 1"
    for index, column in enumerate(PERCENTILE_COLUMNS):
        assert row[column] == STAT.percentiles[index]


def test_build_rows_carries_the_owning_team() -> None:
    assert build_rows([STAT], SNAPSHOT, META)[0]["team"] == "team:recsys"


def test_build_rows_writes_the_snapshot_instant() -> None:
    row = build_rows([STAT], SNAPSHOT, META)[0]
    assert row["date"] == date(2026, 8, 22)
    assert row["partition_ts"] == datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc)


def test_build_rows_keeps_null_metrics_as_none() -> None:
    empty = FeatureStat(
        feature_name="ratio_crnt_min_to_avg_min_full_price_30",
        data_type="double",
        rows_total=1000,
        non_null_count=0,
        null_share=1.0,
        mean=None,
        min_value=None,
        max_value=None,
        percentiles=(None,) * 7,
        duration_ms=10,
        sql="SELECT 1",
    )
    row = build_rows([empty], DAILY, META)[0]
    assert row["mean"] is None
    assert row["p50"] is None
    assert row["null_share"] == 1.0


def test_build_rows_is_empty_without_stats() -> None:
    assert build_rows([], DAILY, META) == []


def test_overwrite_filter_pins_the_snapshot_not_just_the_day() -> None:
    # Снапшотная энтити пишет 8 партиций в сутки: фильтр без partition_ts
    # затирал бы профили предыдущих снапшотов того же дня.
    daily = overwrite_filter_values(DAILY, META)
    snapshot = overwrite_filter_values(SNAPSHOT, META)
    assert daily == {
        "date": date(2026, 8, 22),
        "dag_id": META.dag_id,
        "partition_ts": datetime(2026, 8, 22, tzinfo=timezone.utc),
    }
    assert snapshot["partition_ts"] == datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc)


def main() -> int:
    test_results_table_ref_comes_from_config()
    test_results_catalog_name_comes_from_config()
    test_build_rows_maps_every_field()
    test_build_rows_carries_the_owning_team()
    test_build_rows_writes_the_snapshot_instant()
    test_build_rows_keeps_null_metrics_as_none()
    test_build_rows_is_empty_without_stats()
    test_overwrite_filter_pins_the_snapshot_not_just_the_day()
    print("Feature stats results tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3: Убедись, что тест падает**

Run: `python3 ci_test/test_feature_stats_results.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'feature_stats.results_writer'`

- [ ] **Step 4: Напиши `feature_stats/results_writer.py`**

```python
"""Идемпотентная запись профилей признаков в Iceberg.

Модуль называется results_writer, а не results, потому что каталог
`feature_stats/results/` хранит config.yaml и миграцию самой таблицы и как
namespace-пакет перекрыл бы `feature_stats.results`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import yaml

from dq.results_writer import load_results_catalog  # каталог и креды общие с DQ

from feature_stats.config import PERCENTILE_COLUMNS, StatsContext
from feature_stats.runner import FeatureStat

RESULTS_CONFIG_PATH = Path("feature_stats") / "results" / "config.yaml"


@dataclass(frozen=True)
class RunMeta:
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    run_ts: datetime


def _results_table_config(repo_root: Path) -> dict[str, Any]:
    config = yaml.safe_load((Path(repo_root) / RESULTS_CONFIG_PATH).read_text(encoding="utf-8"))
    return config["table"]


def results_table_ref(repo_root: Path) -> tuple[str, str]:
    """PyIceberg-идентификатор таблицы результатов: ровно (schema, name)."""
    table = _results_table_config(repo_root)
    schema = str(table["schema"]).strip()
    name = str(table["name"]).strip()
    if not schema or not name:
        raise ValueError(f"{RESULTS_CONFIG_PATH}: table.schema и table.name обязательны")
    return schema, name


def results_catalog_name(repo_root: Path) -> str:
    """Имя каталога таблицы результатов из её же config.yaml."""
    catalog = str(_results_table_config(repo_root)["catalog"]).strip()
    if not catalog:
        raise ValueError(f"{RESULTS_CONFIG_PATH}: table.catalog обязателен")
    return catalog


def overwrite_filter_values(ctx: StatsContext, meta: RunMeta) -> dict[str, Any]:
    """Значения, которыми ограничивается идемпотентная перезапись.

    partition_ts обязателен: снапшотная энтити пишет несколько партиций в одни
    календарные сутки, и фильтр по одним date и dag_id оставил бы в таблице
    только последний снапшот дня.
    """
    return {
        "date": ctx.render.partition_date,
        "dag_id": meta.dag_id,
        "partition_ts": ctx.partition_ts,
    }


def build_rows(
    stats: Sequence[FeatureStat], ctx: StatsContext, meta: RunMeta
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stat in stats:
        row: dict[str, Any] = {
            "date": ctx.render.partition_date,
            "partition_ts": ctx.partition_ts,
            "run_ts": meta.run_ts,
            "dag_id": meta.dag_id,
            "task_id": meta.task_id,
            "run_id": meta.run_id,
            "try_number": int(meta.try_number),
            "catalog": ctx.render.catalog_alias,
            "schema_name": ctx.render.schema,
            "table_name": ctx.render.table,
            "team": ctx.render.team,
            "feature_name": stat.feature_name,
            "data_type": stat.data_type,
            "rows_total": int(stat.rows_total),
            "non_null_count": int(stat.non_null_count),
            "null_share": stat.null_share,
            "mean": stat.mean,
            "min_value": stat.min_value,
            "max_value": stat.max_value,
            "duration_ms": int(stat.duration_ms),
            "sql_text": stat.sql,
        }
        for index, column in enumerate(PERCENTILE_COLUMNS):
            row[column] = stat.percentiles[index]
        rows.append(row)
    return rows


def write_results(
    repo_root: Path, stats: Sequence[FeatureStat], ctx: StatsContext, meta: RunMeta
) -> None:
    rows = build_rows(stats, ctx, meta)
    if not rows:
        return

    import pyarrow as pa
    from pyiceberg.expressions import And, EqualTo

    schema, name = results_table_ref(repo_root)
    catalog = load_results_catalog(results_catalog_name(repo_root))
    table = catalog.load_table((schema, name))

    arrow_table = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    values = overwrite_filter_values(ctx, meta)
    table.overwrite(
        arrow_table,
        overwrite_filter=And(
            EqualTo("date", values["date"]),
            EqualTo("dag_id", values["dag_id"]),
            EqualTo("partition_ts", values["partition_ts"]),
        ),
    )
```

- [ ] **Step 5: Убедись, что тест проходит**

Run: `python3 ci_test/test_feature_stats_results.py`
Expected: PASS — `Feature stats results tests completed successfully`

- [ ] **Step 6: Подключи новый корень к обходу конфигов в CI-скриптах**

В `scripts/run_pyspark_migrations.py` строка 134:

```python
    for config_root in ("layers", "datasets", "dq", "feature_stats"):
```

В `scripts/sync_iceberg_maintenance.py` строка 16:

```python
TABLE_CONFIG_ROOTS = ("layers", "datasets", "dq", "feature_stats")
```

`scripts/sync_dbt_sources.py` **не трогай**: таблица результатов, как и `feature_platform_dq_results`, в dbt-trino не уезжает.

- [ ] **Step 7: Проверь, что обход и синхронизация не сломались**

Run:
```bash
python3 ci_test/test_run_pyspark_migrations.py
python3 ci_test/test_sync_iceberg_maintenance.py
python3 ci_test/test_sync_dbt_sources.py
```
Expected: все три PASS. Если `test_sync_iceberg_maintenance.py` ассертит точный список таблиц — добавь в ожидаемый список `feature_platform_feature_stats`.

- [ ] **Step 8: Коммит**

```bash
git add feature_stats/results feature_stats/results_writer.py ci_test/test_feature_stats_results.py scripts/run_pyspark_migrations.py scripts/sync_iceberg_maintenance.py
git commit -m "feat(feature_stats): таблица результатов и идемпотентная запись"
```

---

### Task 5: Фабрика Airflow-таски

**Files:**
- Create: `feature_stats/task.py`
- Test: `ci_test/test_feature_stats_task.py`

**Interfaces:**
- Consumes: `dq.task.parse_partition_value`; `dq.config.trino_catalog_alias`; `feature_stats.config.*`; `feature_stats.runner.run_feature_stats`; `feature_stats.results_writer.{RunMeta, write_results}`.
- Produces:
  - `TASK_ID = "feature_stats"`
  - `partition_instant(partition_date, partition_timestamp) -> datetime`
  - `build_stats_context(config: dict, repo_root: Path, partition_value: Any) -> StatsContext`
  - `build_feature_stats_task(config_path: str, repo_root: str) -> Callable`

- [ ] **Step 1: Напиши падающий тест `ci_test/test_feature_stats_task.py`**

```python
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feature_stats.task import TASK_ID, build_stats_context, partition_instant

DAILY_CONFIG = {
    "table": {
        "catalog": "iceberg",
        "schema": "gold",
        "name": "feature_platform_sku_group_price_features",
        "primary_key": "date,sku_group_id",
        "meta": {"team": "team:search"},
    },
    "feature_stats": {"exclude_columns": []},
}

SNAPSHOT_CONFIG = {
    "table": {
        "catalog": "iceberg",
        "schema": "gold",
        "name": "feature_platform_dynamic_pricing_sku_group_price_features",
        "primary_key": "calculated_at,sku_group_id,promotion_id",
        "meta": {"team": "team:search"},
    },
    "feature_stats": {
        "partition_granularity": "timestamp",
        "partition_column": "calculated_at",
        "snapshot_interval_hours": 3,
        "partition_date_template": "{{ x }}",
    },
}


def test_task_id_is_stable() -> None:
    # На это имя опирается wiring-тест; downstream-сенсоры на него вешать нельзя.
    assert TASK_ID == "feature_stats"


def test_partition_instant_for_a_daily_entity_is_midnight_utc() -> None:
    assert partition_instant(date(2026, 8, 22), None) == datetime(2026, 8, 22, tzinfo=timezone.utc)


def test_partition_instant_for_a_snapshot_entity_is_the_snapshot() -> None:
    naive = datetime(2026, 8, 22, 6, 0, 0)
    assert partition_instant(date(2026, 8, 22), naive) == datetime(
        2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc
    )


def test_build_stats_context_daily() -> None:
    ctx = build_stats_context(DAILY_CONFIG, Path("."), "2026-08-22")
    assert ctx.render.catalog_alias == "dwh-iceberg"
    assert ctx.render.schema == "gold"
    assert ctx.render.table == "feature_platform_sku_group_price_features"
    assert ctx.render.primary_key == ("date", "sku_group_id")
    assert ctx.render.partition_column == "date"
    assert ctx.render.partition_date == date(2026, 8, 22)
    assert ctx.render.partition_granularity == "date"
    assert ctx.render.team == "team:search"
    assert ctx.partition_ts == datetime(2026, 8, 22, tzinfo=timezone.utc)


def test_build_stats_context_snapshot() -> None:
    ctx = build_stats_context(SNAPSHOT_CONFIG, Path("."), "2026-08-22 06:00:00")
    assert ctx.render.partition_column == "calculated_at"
    assert ctx.render.partition_granularity == "timestamp"
    assert ctx.render.partition_timestamp == datetime(2026, 8, 22, 6, 0, 0)
    assert ctx.render.snapshot_interval_hours == 3
    assert ctx.partition_ts == datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc)


def test_build_stats_context_defaults_the_team() -> None:
    config = {"table": {**DAILY_CONFIG["table"]}}
    config["table"].pop("meta")
    assert build_stats_context(config, Path("."), "2026-08-22").render.team == "team:search"


def main() -> int:
    test_task_id_is_stable()
    test_partition_instant_for_a_daily_entity_is_midnight_utc()
    test_partition_instant_for_a_snapshot_entity_is_the_snapshot()
    test_build_stats_context_daily()
    test_build_stats_context_snapshot()
    test_build_stats_context_defaults_the_team()
    print("Feature stats task tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Убедись, что тест падает**

Run: `python3 ci_test/test_feature_stats_task.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'feature_stats.task'`

- [ ] **Step 3: Напиши `feature_stats/task.py`**

```python
"""Фабрика Airflow-таски feature_stats."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from dq.config import RenderContext, trino_catalog_alias
from dq.task import parse_partition_value

from feature_stats.config import DEFAULT_TEAM, StatsContext, load_feature_stats_settings
from feature_stats.results_writer import RunMeta, write_results
from feature_stats.runner import run_feature_stats

TASK_ID = "feature_stats"


def partition_instant(partition_date: date, partition_timestamp: datetime | None) -> datetime:
    """Канонический момент партиции в UTC.

    У снапшотной энтити это записанный снапшот, у дневной — полночь её партиции.
    Заполнен всегда: он входит в ключ таблицы результатов и в фильтр перезаписи,
    а нулевое значение потребовало бы ветвления на IsNull в обоих местах.
    """
    if partition_timestamp is not None:
        return partition_timestamp.replace(tzinfo=timezone.utc)
    return datetime.combine(partition_date, time.min, tzinfo=timezone.utc)


def build_stats_context(config: dict[str, Any], repo_root: Path, partition_value: Any) -> StatsContext:
    table = config["table"]
    settings = load_feature_stats_settings(config)
    meta = table.get("meta") or {}
    partition_date, partition_timestamp = parse_partition_value(
        partition_value, settings.partition_granularity
    )
    render = RenderContext(
        catalog_alias=trino_catalog_alias(repo_root, str(table["catalog"])),
        schema=str(table["schema"]),
        table=str(table["name"]),
        primary_key=tuple(
            column.strip() for column in str(table["primary_key"]).split(",") if column.strip()
        ),
        partition_column=settings.partition_column,
        partition_date=partition_date,
        # Профиль всегда считается по одной партиции: table-wide скан 30M строк
        # на каждый DAG-ран не окупается ничем.
        scope="partition",
        sample_rows=0,
        team=str(meta.get("team") or DEFAULT_TEAM),
        partition_granularity=settings.partition_granularity,
        partition_timestamp=partition_timestamp,
        snapshot_interval_hours=settings.snapshot_interval_hours,
    )
    return StatsContext(
        render=render, partition_ts=partition_instant(partition_date, partition_timestamp)
    )


def build_feature_stats_task(config_path: str, repo_root: str) -> Callable:
    """Возвращает готовую Airflow-таску feature_stats для DAG'а энтити."""
    from airflow.providers.trino.hooks.trino import TrinoHook
    from airflow.sdk import get_current_context, task
    from airflow_commons.helpers.oncall import send_oncall_notification

    config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    settings = load_feature_stats_settings(config)
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
    def feature_stats(partition_date_value: str) -> None:
        import logging

        logger = logging.getLogger("airflow.task")
        ctx = build_stats_context(config, Path(repo_root), partition_date_value)

        hook = TrinoHook(trino_conn_id=settings.trino_conn_id)

        def query(sql: str) -> list:
            logger.info("Feature stats query:\n%s", sql)
            return hook.get_records(sql)

        stats = run_feature_stats(settings, ctx, query)
        logger.info(
            "Профилей посчитано: %s для %s.%s.%s за %s",
            len(stats),
            ctx.render.catalog_alias,
            ctx.render.schema,
            ctx.render.table,
            ctx.partition_ts.isoformat(),
        )
        if not stats:
            return

        airflow_context = get_current_context()
        task_instance = airflow_context["task_instance"]
        write_results(
            Path(repo_root),
            stats,
            ctx,
            RunMeta(
                dag_id=task_instance.dag_id,
                task_id=TASK_ID,
                run_id=task_instance.run_id,
                try_number=int(task_instance.try_number),
                run_ts=airflow_context["logical_date"],
            ),
        )

    return feature_stats
```

- [ ] **Step 4: Убедись, что тест проходит**

Run: `python3 ci_test/test_feature_stats_task.py`
Expected: PASS — `Feature stats task tests completed successfully`

- [ ] **Step 5: Коммит**

```bash
git add feature_stats/task.py ci_test/test_feature_stats_task.py
git commit -m "feat(feature_stats): фабрика Airflow-таски"
```

---

### Task 6: CI-валидатор конфигов

**Files:**
- Create: `scripts/validate_feature_stats_configs.py`
- Modify: `.drone.yaml` (после шага `validate dq configs`, строка ~34)
- Test: `ci_test/test_validate_feature_stats_configs.py`

**Interfaces:**
- Consumes: `feature_stats.config.{FeatureStatsConfigError, load_feature_stats_settings}`.
- Produces:
  - `ENTITY_CONFIG_ROOTS: tuple[str, ...]`
  - `PARTITION_KEYS: tuple[str, ...]`
  - `discover_entity_configs(repo_root: Path) -> list[Path]`
  - `migration_columns(entity_dir: Path) -> set[str]`
  - `validate_config(config_path: Path) -> list[str]`

- [ ] **Step 1: Напиши падающий тест `ci_test/test_validate_feature_stats_configs.py`**

```python
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from validate_feature_stats_configs import discover_entity_configs, validate_config

TABLE_BLOCK = """table:
  catalog: iceberg
  schema: gold
  name: feature_platform_demo
  primary_key: date,sku_group_id
  meta:
    team: team:search
"""

MIGRATION = """CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'd',
    sku_group_id BIGINT COMMENT 'k',
    category_id BIGINT COMMENT 'c',
    conv_imp2order_7 DOUBLE COMMENT 'f'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
"""


def write_entity(root: Path, config_body: str) -> Path:
    entity = root / "layers" / "gold" / "demo" / "v1"
    (entity / "migrations").mkdir(parents=True)
    (entity / "migrations" / "create_table.sql").write_text(MIGRATION, encoding="utf-8")
    config_path = entity / "config.yaml"
    config_path.write_text(TABLE_BLOCK + config_body, encoding="utf-8")
    return config_path


def test_valid_config_has_no_problems() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "feature_stats:\n  exclude_columns:\n    - category_id\n",
        )
        assert validate_config(config_path) == []


def test_exclude_column_absent_from_migrations_is_reported() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "feature_stats:\n  exclude_columns:\n    - categoryy_id\n",
        )
        problems = validate_config(config_path)
        assert len(problems) == 1
        assert "categoryy_id" in problems[0]


def test_partition_settings_diverging_from_dq_are_reported() -> None:
    # Разные партиции у dq и feature_stats в одном DAG-ране — всегда ошибка:
    # профиль считался бы не по тем данным, что проверял DQ.
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "dq:\n  partition_date_template: 'A'\n"
            "feature_stats:\n  partition_date_template: 'B'\n",
        )
        problems = validate_config(config_path)
        assert len(problems) == 1
        assert "partition_date_template" in problems[0]


def test_matching_partition_settings_are_accepted() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "dq:\n  partition_column: date\n  partition_date_template: 'A'\n"
            "feature_stats:\n  partition_column: date\n  partition_date_template: 'A'\n",
        )
        assert validate_config(config_path) == []


def test_invalid_block_is_reported_as_one_problem() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(Path(tmp), "feature_stats:\n  percentiles: [0.5]\n")
        problems = validate_config(config_path)
        assert len(problems) == 1
        assert "percentiles" in problems[0]


def test_config_without_a_table_block_is_skipped() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        entity = Path(tmp) / "layers" / "gold" / "demo" / "v1"
        entity.mkdir(parents=True)
        config_path = entity / "config.yaml"
        config_path.write_text("resources:\n  path: x\n", encoding="utf-8")
        assert validate_config(config_path) == []


def test_repository_configs_are_all_valid() -> None:
    problems: list[str] = []
    for config_path in discover_entity_configs(Path(".")):
        problems.extend(validate_config(config_path))
    assert problems == [], problems


def main() -> int:
    test_valid_config_has_no_problems()
    test_exclude_column_absent_from_migrations_is_reported()
    test_partition_settings_diverging_from_dq_are_reported()
    test_matching_partition_settings_are_accepted()
    test_invalid_block_is_reported_as_one_problem()
    test_config_without_a_table_block_is_skipped()
    test_repository_configs_are_all_valid()
    print("validate_feature_stats_configs tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Убедись, что тест падает**

Run: `python3 ci_test/test_validate_feature_stats_configs.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'validate_feature_stats_configs'`

- [ ] **Step 3: Напиши `scripts/validate_feature_stats_configs.py`**

```python
#!/usr/bin/env python3
"""CI-гейт: проверяет корректность блоков feature_stats: во всех энтити-конфигах."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feature_stats.config import (  # noqa: E402
    FeatureStatsConfigError,
    load_feature_stats_settings,
)

ENTITY_CONFIG_ROOTS = ("layers", "datasets")
COLUMN_DEFINITION = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+[A-Za-z]", re.MULTILINE)
ADD_COLUMN = re.compile(r"ADD\s+COLUMNS?\s*\(?\s*([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)

# Поля, по которым блоки dq: и feature_stats: обязаны совпадать: они определяют,
# какую именно партицию смотрит таска.
PARTITION_KEYS = (
    "partition_column",
    "partition_granularity",
    "partition_date_template",
    "snapshot_interval_hours",
)


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


def validate_config(config_path: Path) -> list[str]:
    problems: list[str] = []
    config: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if "table" not in config:
        return problems

    try:
        settings = load_feature_stats_settings(config)
    except FeatureStatsConfigError as error:
        return [f"{config_path}: {error}"]

    if not settings.enabled:
        readme = config_path.parent / "README.md"
        readme_text = readme.read_text(encoding="utf-8") if readme.is_file() else ""
        if "feature_stats" not in readme_text.lower():
            problems.append(
                f"{config_path}: feature_stats.enabled=false, но в README нет объяснения, "
                "почему расчёт статистик выключен"
            )
        return problems

    problems.extend(_partition_divergence(config_path, config))

    known_columns = migration_columns(config_path.parent)
    if known_columns:
        for column in settings.exclude_columns:
            if column.lower() not in known_columns:
                problems.append(
                    f"{config_path}: feature_stats.exclude_columns ссылается на колонку "
                    f"{column!r}, которой нет в миграциях энтити"
                )
    return problems


def _partition_divergence(config_path: Path, config: dict[str, Any]) -> list[str]:
    dq_block = config.get("dq")
    stats_block = config.get("feature_stats")
    if not isinstance(dq_block, dict) or not isinstance(stats_block, dict):
        return []

    problems: list[str] = []
    for key in PARTITION_KEYS:
        if key not in dq_block and key not in stats_block:
            continue
        if dq_block.get(key) != stats_block.get(key):
            problems.append(
                f"{config_path}: {key} расходится между блоками dq: ({dq_block.get(key)!r}) и "
                f"feature_stats: ({stats_block.get(key)!r}). Обе таски обязаны смотреть на одну "
                "партицию, иначе профиль посчитан не по тем данным, что проверял DQ."
            )
    return problems


def main() -> int:
    repo_root = Path(".")
    problems: list[str] = []
    configs = discover_entity_configs(repo_root)
    for config_path in configs:
        problems.extend(validate_config(config_path))

    print(f"Проверено конфигов: {len(configs)}")
    if problems:
        for problem in problems:
            print(f"ERROR {problem}")
        return 1
    print("Все блоки feature_stats: валидны")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Убедись, что тест проходит и валидатор чист на репозитории**

Run:
```bash
python3 ci_test/test_validate_feature_stats_configs.py
python3 scripts/validate_feature_stats_configs.py
```
Expected: PASS и `Все блоки feature_stats: валидны` (на этом шаге блоков ещё нет — валидатор просто ничего не находит).

- [ ] **Step 5: Добавь шаг в `.drone.yaml`**

Сразу после шага `validate dq configs`:

```yaml
  - name: validate feature stats configs
    image: python:3.11-slim
    commands:
      - echo "=== validate feature stats configs ==="
      - pip install --no-cache-dir PyYAML==6.0.2
      - python scripts/validate_feature_stats_configs.py
```

- [ ] **Step 6: Коммит**

```bash
git add scripts/validate_feature_stats_configs.py ci_test/test_validate_feature_stats_configs.py .drone.yaml
git commit -m "feat(feature_stats): CI-валидатор блоков конфига"
```

---

### Task 7: Проверка отрендеренного SQL на живом Trino

**Files:**
- Create: (временный скрипт в скрэтчпаде, в репозиторий не коммитится)
- Modify: `docs/superpowers/plans/2026-08-23-feature-stats.md` (запиши результат замера сюда же, в конец задачи)

**Interfaces:**
- Consumes: `feature_stats.query.render_stats_query`, `feature_stats.task.build_stats_context`.
- Produces: решение о дефолте `columns_per_query` для Task 8.

Локально диалект Trino не исполняется ничем, поэтому это единственный момент, где проверяется, что запрос вообще выполним. `AGENTS.md` требует такого прогона до объявления работы законченной.

- [ ] **Step 1: Отрендери SQL для самой широкой таблицы**

```bash
python3 - <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, '.')
import yaml
from feature_stats.task import build_stats_context
from feature_stats.query import render_columns_query, render_stats_query

config = yaml.safe_load(Path(
    "layers/gold/sku_group_id/sku_group_search_conversion_features/v2/config.yaml"
).read_text(encoding="utf-8"))
config.setdefault("feature_stats", {"exclude_columns": ["category_id"]})
ctx = build_stats_context(config, Path("."), "2026-08-22")
print(render_columns_query(ctx))
PY
```

- [ ] **Step 2: Выполни запрос списка колонок в Trino**

Через MCP-инструмент `mcp__trino__execute_query`. Ожидание: 90+ строк с именами и типами.

Если Trino недоступен (нет VPN/DNS — ошибка `Failed to resolve trino.prod-data.internal.daymarket.uz`), **останови выполнение плана и скажи об этом человеку**. Не пропускай эту задачу и не переходи к Task 8: без неё нет ответа на вопрос, строится ли план из 445 агрегатов.

- [ ] **Step 3: Отрендери и выполни полный запрос статистик**

Собери список признаков из результата шага 2 (числовые типы, минус `date`, `sku_group_id`, `category_id`), передай в `render_stats_query`, выполни в Trino. Замерь время.

- [ ] **Step 4: Повтори для снапшотной энтити**

```bash
python3 - <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, '.')
import yaml
from feature_stats.task import build_stats_context
from feature_stats.query import render_stats_query

config = yaml.safe_load(Path(
    "layers/gold/calculated_at_sku_group_id_promotion_id/"
    "dynamic_pricing_sku_group_price_features/v1/config.yaml"
).read_text(encoding="utf-8"))
config["feature_stats"] = {
    "partition_granularity": "timestamp",
    "partition_column": "calculated_at",
    "snapshot_interval_hours": 3,
    "partition_date_template": "{{ x }}",
}
ctx = build_stats_context(config, Path("."), "2026-08-22 06:00:00")
print(render_stats_query(ctx, ["avg_sell_price", "min_discount"]))
PY
```

Выполни в Trino и сверь `rows_total` с прямым `SELECT count(*) ... WHERE calculated_at = TIMESTAMP '2026-08-22 06:00:00 UTC'`. Числа обязаны совпасть: расхождение означает, что литерал коэрсится по таймзоне сессии и указывает на соседний снапшот.

- [ ] **Step 5: Зафиксируй результат**

Допиши в конец этой задачи в файле плана: время выполнения обоих запросов, построился ли план из 445 агрегатов, и итоговое решение по `columns_per_query` (`null` для всех семи таблиц либо конкретное число для конкретных).

- [ ] **Step 6: Коммит**

```bash
git add docs/superpowers/plans/2026-08-23-feature-stats.md
git commit -m "docs(feature_stats): результат прогона SQL на живом Trino"
```

#### Результат прогона (2026-08-23)

Все запросы выполнены через `mcp__trino__execute_query` против живого Trino
(`"dwh-iceberg"` catalog). Точное время исполнения взято из
`system.runtime.queries` (`created`/`"end"`), а не из настенных часов агента —
собственная генерация текста запроса добавляет шум, реальная длительность на
стороне Trino надёжнее.

**Самая широкая таблица** —
`gold.feature_platform_sku_group_search_conversion_features_v2`
(партиция `2026-08-22`):

- `information_schema.columns` вернула 92 колонки (`date`, `sku_group_id`,
  `category_id` + 89 числовых признаков), как и ожидалось (~90+).
- `feature_stats.runner.select_feature_columns` с `settings.exclude_columns =
  ("category_id",)`, применённый к этому списку колонок, выбрал ровно **89**
  признаков — совпадает с ручным подсчётом в спеке (~89).
- `render_stats_query` для этих 89 признаков породил план из
  `1 + 89 × 5 = 446` агрегатных выражений (`count(*)` + 89 ×
  `count/avg/min/max/approx_percentile`).
- Запрос **построился и выполнился успешно** (`state = FINISHED`,
  `query_id = 20260823_200819_10737_z6v7w`).
  **Время выполнения: 20:08:23.500 → 20:08:41.985 UTC ≈ 18.5 секунды.**
- **Уточнение по точности воспроизведения.** Исполненный текст не был
  байт-в-байт выводом `render_stats_query` — агент вручную перенёс
  30 КБ SQL-строки в вызов MCP-инструмента (см. упоминание "искажённой
  первой попытки" выше), и это ручное перенесение внесло одно расхождение.
  Сверка исполненного SQL из `system.runtime.queries` (по `query_id`) с
  чистым локальным рендером `render_stats_query`, чанк за чанком:
  - чанки 1–6 (первые 30 000 символов, после нормализации пробелов) —
    **побайтово идентичны**;
  - md5 упорядоченного списка из 89 имён признаков — **идентичен** с обеих
    сторон;
  - общая нормализованная длина совпадает с поправкой на добавленный
    MCP-инструментом `LIMIT 10`;
  - **единственное различие** — в хвосте, для последнего признака:
    исполненный запрос содержит
    `min(CAST("imp2order_30_to_21" AS DOUBLE)) AS max_88`, тогда как
    рендерер выдаёт `min(CAST("imp2order_30_to_21" AS DOUBLE)) AS min_88`
    (дублирующийся алиас `max_88` вместо `min_88`).
  Выводы это не обесценивает: дублирующиеся алиасы в списке проекций
  легальны в Trino, набор всех 446 агрегатных выражений и их аргументов
  не изменился, поэтому форма и стоимость плана не затронуты и замер
  ≈18.5 секунды валиден для продовой версии запроса. `parse_stats_row`
  разбирает строку результата **по позиции**, а не по алиасу, поэтому
  раунд-трип на 89 `FeatureStat` и спот-чек `imp2order_30_to_21`
  (min=6.78e-04, max=21.75) ниже остаются корректными.
- `rows_total = 1 965 579`.
- Спот-чек значений (min ≤ p05 ≤ p50 ≤ p95 ≤ max, перцентили
  неубывающие — везде выполнено):
  - `smooth_conv_imp2order_3`: min=4.18e-08, p05=9.86e-06, p50=2.356e-04,
    p95=2.410e-03, max=3.560 — плотная колонка, всюду not null.
  - `skg_days_since_last_impression`: min=1, p05..p25=1 (масса значений у
    минимума), p50=1, p75=11.7, p95=70.6, max=90 — правдоподобно для
    "дней с последнего показа".
  - `imp2order_30_to_21`: null_share≈0.826 (разреженная by design, как и
    задокументировано в `config.yaml` таблицы), min=6.78e-04, max=21.75,
    перцентили неубывающие в границах min/max.
- Раунд-трип через `feature_stats.runner.parse_stats_row` с фактической
  строкой Trino и списком из 89 `(name, type)`: получено ровно **89**
  `FeatureStat`, без исключений; ширина строки (446 значений) совпала с
  ожидаемой `1 + len(batch) * VALUES_PER_COLUMN`.

**Снапшотная энтити** —
`gold.feature_platform_dynamic_pricing_sku_group_price_features`
(`partition_granularity: timestamp`, `partition_column: calculated_at`,
`snapshot_interval_hours: 3`), признаки `avg_sell_price`, `min_discount`:

- Партиция из брифа, `2026-08-22 06:00:00`, оказалась **пустой**: у таблицы
  в этом окружении данные лежат только за `2026-06-30 06:00:00` …
  `2026-07-22 06:00:00 UTC` (проверено `min/max(calculated_at)`), то есть на
  месяц раньше запрошенной даты. И `render_stats_query`
  (`rows_total = 0`), и прямой `SELECT count(*) ... WHERE calculated_at =
  TIMESTAMP '2026-08-22 06:00:00 UTC'` (`cnt = 0`) синхронно вернули ноль —
  формальное совпадение выполнено, но ноль на обеих сторонах не проверяет
  риск коэрсии по таймзоне (0 = 0 истинно даже при сдвиге на соседнюю
  пустую партицию). Время: `query_id = 20260823_201018_10875_z6v7w`,
  `created 20:10:21.981 → end 20:10:22.277 UTC ≈ 0.3 секунды` (тривиально
  быстро — метаданные, скан не потребовался).
- Чтобы получить содержательную проверку, тот же прогон повторён на
  последней реально существующей партиции той же таблицы,
  `2026-07-22 06:00:00`:
  - `render_stats_query` → `rows_total = 15 657 791`.
    `query_id = 20260823_201117_10994_z6v7w`,
    `created 20:11:21.773 → end 20:11:29.778 UTC ≈ 8.0 секунды`.
  - Прямой `SELECT count(*) ... WHERE calculated_at = TIMESTAMP
    '2026-07-22 06:00:00 UTC'` → `cnt = 15 657 791`. **Числа совпали
    точно.**
  - Соседние снапшоты того же дня дают другие числа
    (`00:00:00 UTC → 15 653 152`, `03:00:00 UTC → 15 654 063`,
    `06:00:00 UTC → 15 657 791`), поэтому точное совпадение на
    `06:00:00 UTC` — не случайность, а доказательство того, что литерал
    `TIMESTAMP '... UTC'` (тип `timestamp(6) with time zone`, колонка
    `calculated_at` того же типа) не коэрсится сессионной таймзоной
    (`Europe/Moscow`) и указывает ровно на запрошенный снапшот, а не на
    соседний. Самый опасный сценарий из брифа не подтвердился.
  - Спот-чек: `avg_sell_price` (min=10.0, p05=14 780, p50=107 412,
    p95=1 454 741, max=9 999 999 900 — судя по разрыву между p95 и max,
    в данных есть выброс-заглушка, но перцентили неубывающие и min/max
    границы соблюдены); `min_discount` (min=0, все семь перцентилей = 0 —
    колонка сильно вырождена к нулю, max=84 409 000 — тот же тип выброса).
    Это наблюдение о качестве данных, не о коде `feature_stats`.
  - Раунд-трип через `parse_stats_row` с этой строкой и `batch =
    [("avg_sell_price", "double"), ("min_discount", "double")]`: получено
    ровно 2 `FeatureStat`, без исключений.

**Вывод про `columns_per_query`.** План из 445 агрегатных выражений (самая
широкая из семи таблиц) строится и выполняется Trino без проблем за ~18.5
секунды — на порядок дешевле, чем стоил бы дополнительный полный скан
партиции ради разбиения на партии.

Ширина остальных шести таблиц — не предположение, а офлайн-проверка:
контроллер разобрал миграции каждой энтити и прогнал тот же
`feature_stats.runner.is_numeric` и те же правила ключа/исключений, что
использует раннер, — без обращения к Trino:

| таблица | признаков | агрегатов (1 + n×5) |
|---|---|---|
| `sku_group_search_conversion_features_v2` | 89 | 446 |
| `sku_group_query_atc_order_features_v2` | 69 | 346 |
| `search_query_atc_features` | 24 | 121 |
| `feedback_sku_group_id` | 17 | 86 |
| `dynamic_pricing_sku_group_price_features` | 9 | 46 |
| `sku_group_stock_features` | 8 | 41 |
| `sku_group_price_features` | 6 | 31 |

Таблица, которую замерили на живом Trino (`sku_group_search_conversion_features_v2`,
89 признаков / 446 агрегатов), — самая широкая по этому списку, так что
измеренные ~18.5 секунды — это верхняя граница по стоимости плана для всех
семи таблиц, а не догадка по необмеренным. **Решение: `columns_per_query:
null` (один запрос на таблицу) остаётся дефолтом для всех семи таблиц в
Task 8; ни для одной не нужно явно задавать `columns_per_query` в
`config.yaml`.**

#### Дозамер: самая большая по строкам таблица (2026-08-23, final-review fix wave)

Замер 18.5 секунды выше ограничивает **ширину плана** (число агрегатных выражений
в одном запросе Trino) — он снят на таблице с максимальным числом признаков среди
семи (`sku_group_search_conversion_features_v2`, 89 признаков / 446 агрегатов, но
всего 1 965 579 строк в партиции). Он ничего не говорит о **стоимости скана**,
которая определяется объёмом партиции. `feature_platform_search_query_atc_features`
держит 30–32M строк на партицию — ~16× больше — при всего 24 признаках / 121
агрегате, и именно эту таблицу design-спека называла риском нагрузки, который так
и остался неизмеренным до этой волны фиксов.

Запрос собран из поставляемого кода, не вручную: `feature_stats.task.build_stats_context`
+ `feature_stats.config.load_feature_stats_settings` для энтити
`layers/gold/query/search_query_atc_features/v1`, партиция `2026-08-22`;
список колонок получен `render_columns_query` через `information_schema` (26 колонок:
`date`, `query`, 24 `double`); `feature_stats.runner.select_feature_columns` отобрал
ровно **24** признака (совпадает с офлайн-подсчётом в таблице выше); SQL собран
`feature_stats.query.render_stats_query` для этих 24 признаков — план из
`1 + 24 × 5 = 121` агрегатных выражений, **построился без ошибок**.

Запрос выполнен через `mcp__trino__execute_query` **ровно один раз** (без повторных
замеров). Длительность взята из `system.runtime.queries` по `query_id`, а не с настенных
часов агента:

- `query_id = 20260823_205247_15463_z6v7w`, `state = FINISHED`.
- `created 2026-08-23 20:52:51.664 UTC → end 2026-08-23 20:53:28.216 UTC`.
- **Длительность: 36 552 мс ≈ 36.6 секунды.**
- `rows_total = 32 405 436`.

**Исправленное утверждение о том, что чем ограничено.** Прежний вывод «18.5 секунды —
верхняя граница по стоимости для всех семи таблиц» был неверен: он подменял стоимость
плана стоимостью скана. Правильно так:

- Замер на `sku_group_search_conversion_features_v2` (446 агрегатов, 1.97M строк,
  18.5с) — верхняя граница **ширины плана**: подтверждает, что Trino строит и
  выполняет запрос с самым большим числом агрегатных выражений среди семи таблиц
  без ошибок и в разумное время.
- Этот новый замер на `feature_platform_search_query_atc_features` (121 агрегат,
  32.4M строк, 36.6с) — верхняя граница **стоимости скана** среди измеренных: это
  самая большая по строкам партиция среди семи таблиц, и несмотря на втрое более
  узкий план (121 агрегат против 446), запрос занял почти вдвое дольше (36.6с
  против 18.5с) — прямое подтверждение, что при этих объёмах именно объём
  партиции, а не число агрегатных выражений в плане, определяет стоимость.
- Ни один из двух замеров не покрывает комбинацию «много строк И много
  признаков» — среди семи таблиц такой комбинации нет (самая широкая по
  признакам таблица на 16× меньше по строкам, чем самая большая по строкам), так
  что для реально подключённых семи таблиц оба измерения вместе дают достаточное
  покрытие, но экстраполировать на гипотетическую восьмую таблицу с обоими
  параметрами сразу нельзя.

**Стоит ли беспокоиться о 36.6 секунды на общем кластере.** Прямо: да, стоит держать
в поле зрения, но не как повод останавливать мёрж. 36.6 секунды — это один полный
скан 32.4M строк с 121 агрегатным выражением, занимающий воркер-слот и ресурсы
шеред-кластера Trino почти на 40 секунд за один прогон DAG'а; это заметно дороже,
чем предполагал прежний (ошибочный) вывод «весь набор ограничен 18.5 секунды», и
это самая тяжёлая по факту измеренная позиция среди семи таблиц. Она укладывается в
`query_timeout_seconds` (дефолт 600с) с большим запасом, и `feature_stats` не
блокирует публикацию фич при падении/таймауте, так что риска для аплоада нет. Но
если несколько таких тяжёлых `feature_stats`-тасок разных DAG'ов совпадут по
времени на проде, это ощутимая, не бесплатная нагрузка на общий Trino, а не
«секунды, о которых можно не думать», как формулировал прежний вывод.

---

### Task 8: Проводка семи DAG'ов

**Files:**
- Modify (по паре `config.yaml` + `dag.py` на каждую энтити):
  - `layers/gold/query/search_query_atc_features/v1/`
  - `layers/gold/query_sku_group_id/sku_group_query_atc_order_features/v2/`
  - `layers/gold/sku_group_id/sku_group_search_conversion_features/v2/`
  - `layers/gold/sku_group_id/sku_group_stock_features/v1/`
  - `layers/gold/sku_group_id/sku_group_price_features/v1/`
  - `layers/gold/sku_group_id/feedback_sku_group_id/v1/`
  - `layers/gold/calculated_at_sku_group_id_promotion_id/dynamic_pricing_sku_group_price_features/v1/`
- Test: `ci_test/test_feature_stats_task_wiring.py`

**Interfaces:**
- Consumes: `feature_stats.task.build_feature_stats_task`.
- Produces: рабочие DAG'и; никаких новых Python-символов.

- [ ] **Step 1: Напиши падающий тест `ci_test/test_feature_stats_task_wiring.py`**

```python
import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

# Все gold-источники ranking- и dynamic-pricing-аплоада.
UPLOAD_SOURCE_DAGS = (
    "layers/gold/query/search_query_atc_features/v1/dag.py",
    "layers/gold/query_sku_group_id/sku_group_query_atc_order_features/v2/dag.py",
    "layers/gold/sku_group_id/sku_group_search_conversion_features/v2/dag.py",
    "layers/gold/sku_group_id/sku_group_stock_features/v1/dag.py",
    "layers/gold/sku_group_id/sku_group_price_features/v1/dag.py",
    "layers/gold/sku_group_id/feedback_sku_group_id/v1/dag.py",
    (
        "layers/gold/calculated_at_sku_group_id_promotion_id/"
        "dynamic_pricing_sku_group_price_features/v1/dag.py"
    ),
)

SNAPSHOT_DAG = UPLOAD_SOURCE_DAGS[-1]


def calls(dag_path: Path, name: str) -> bool:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name:
            return True
    return False


def test_every_upload_source_dag_builds_the_task() -> None:
    for relative in UPLOAD_SOURCE_DAGS:
        dag_path = Path(relative)
        assert dag_path.is_file(), relative
        assert calls(dag_path, "build_feature_stats_task"), f"{relative}: нет build_feature_stats_task"


def test_task_runs_in_parallel_with_dq_and_has_no_downstream() -> None:
    for relative in UPLOAD_SOURCE_DAGS:
        text = Path(relative).read_text(encoding="utf-8")
        assert ">> [dq_task, stats_task]" in text, f"{relative}: таска не параллельна dq"
        # Downstream на stats_task вешать нельзя: падение статистики не должно
        # блокировать публикацию фич, аплоад ждёт именно dq.
        assert "stats_task >>" not in text, f"{relative}: у stats_task не должно быть downstream"


def test_every_upload_source_dag_has_a_feature_stats_block() -> None:
    for relative in UPLOAD_SOURCE_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert isinstance(config.get("feature_stats"), dict), f"{config_path}: нет блока feature_stats:"


def test_partition_settings_match_the_dq_block() -> None:
    keys = ("partition_column", "partition_granularity", "partition_date_template",
            "snapshot_interval_hours")
    for relative in UPLOAD_SOURCE_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        for key in keys:
            assert config["dq"].get(key) == config["feature_stats"].get(key), (
                f"{config_path}: {key} расходится между dq: и feature_stats:"
            )


def test_snapshot_dag_passes_a_timestamp_template_to_both_tasks() -> None:
    """У снапшотной энтити константа называется DQ_PARTITION_TIMESTAMP, а не ..._DATE."""
    text = Path(SNAPSHOT_DAG).read_text(encoding="utf-8")
    template = 'data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S")'
    assert template in text
    assert (
        "build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_TIMESTAMP)" in text
    ), "статистике снапшотной энтити нужен тот же timestamp-шаблон, что и dq"


def test_daily_dags_pass_the_date_template_to_both_tasks() -> None:
    for relative in UPLOAD_SOURCE_DAGS[:-1]:
        text = Path(relative).read_text(encoding="utf-8")
        assert (
            "build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)" in text
        ), f"{relative}: статистика должна получать ту же константу партиции, что и dq"


def test_only_the_conversion_table_excludes_a_column() -> None:
    """category_id — единственная числовая не-фича среди семи таблиц."""
    for relative in UPLOAD_SOURCE_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        excluded = config["feature_stats"].get("exclude_columns") or []
        if "sku_group_search_conversion_features" in relative:
            assert excluded == ["category_id"], config_path
        else:
            assert excluded == [], config_path


def main() -> int:
    test_every_upload_source_dag_builds_the_task()
    test_task_runs_in_parallel_with_dq_and_has_no_downstream()
    test_every_upload_source_dag_has_a_feature_stats_block()
    test_partition_settings_match_the_dq_block()
    test_snapshot_dag_passes_a_timestamp_template_to_both_tasks()
    test_daily_dags_pass_the_date_template_to_both_tasks()
    test_only_the_conversion_table_excludes_a_column()
    print("Feature stats task wiring tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Убедись, что тест падает**

Run: `python3 ci_test/test_feature_stats_task_wiring.py`
Expected: FAIL — `нет build_feature_stats_task` на первом же DAG'е.

- [ ] **Step 3: Добавь блок `feature_stats:` в шесть дневных конфигов**

В каждый из шести `config.yaml` (все, кроме dynamic pricing) допиши в конец. `partition_date_template` **копируй из блока `dq:` того же файла дословно** — валидатор и wiring-тест сверяют их посимвольно.

```yaml
feature_stats:
  trino_conn_id: trino_search
  # Дословно совпадает с dq.partition_date_template: обе таски обязаны смотреть
  # на одну партицию, иначе профиль посчитан не по тем данным, что проверял DQ.
  partition_date_template: '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  exclude_columns: []
```

Для `layers/gold/sku_group_id/sku_group_search_conversion_features/v2/config.yaml` вместо пустого списка:

```yaml
  # category_id — идентификатор категории, а не признак: профиль распределения
  # по нему бессмысленен. Остальные BIGINT/INT-колонки таблицы — настоящие признаки.
  exclude_columns:
    - category_id
```

- [ ] **Step 4: Добавь блок в конфиг снапшотной энтити**

`layers/gold/calculated_at_sku_group_id_promotion_id/dynamic_pricing_sku_group_price_features/v1/config.yaml`:

```yaml
feature_stats:
  trino_conn_id: trino_search
  # Все четыре поля дословно повторяют блок dq: энтити пишет 8 партиций в сутки,
  # и профиль обязан относиться ровно к тому снапшоту, который проверял DQ.
  partition_granularity: timestamp
  partition_column: calculated_at
  snapshot_interval_hours: 3
  partition_date_template: '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
  exclude_columns: []
```

- [ ] **Step 5: Проверь валидатор на реальных конфигах**

Run: `python3 scripts/validate_feature_stats_configs.py`
Expected: `Проверено конфигов: N` и `Все блоки feature_stats: валидны`. Любое `ERROR ... расходится между блоками` означает, что шаблон скопирован неточно — почини конфиг, не валидатор.

- [ ] **Step 6: Проведи таску в семи `dag.py`**

В каждом файле добавь импорт рядом с существующим `from dq.task import build_dq_task`:

```python
from feature_stats.task import build_feature_stats_task
```

И замени строку связывания. Было:

```python
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    collect_features >> dq_task
```

Стало:

```python
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: аплоад ждёт таску dq,
    # поэтому падение профилей не блокирует публикацию фич.
    collect_features >> [dq_task, stats_task]
```

Фактические имена по файлам — сверено с кодом, вслепую не подставляй:

| DAG | upstream-таска | константа шаблона |
|---|---|---|
| `query/search_query_atc_features/v1` | `collect_features` | `DQ_PARTITION_DATE` |
| `query_sku_group_id/sku_group_query_atc_order_features/v2` | `collect_features` | `DQ_PARTITION_DATE` |
| `sku_group_id/sku_group_search_conversion_features/v2` | `collect_features` | `DQ_PARTITION_DATE` |
| `sku_group_id/sku_group_stock_features/v1` | `collect_features` | `DQ_PARTITION_DATE` |
| `sku_group_id/sku_group_price_features/v1` | `collect_features` | `DQ_PARTITION_DATE` |
| `sku_group_id/feedback_sku_group_id/v1` | `collect_feedback_stats` | `DQ_PARTITION_DATE` |
| `calculated_at_.../dynamic_pricing_sku_group_price_features/v1` | `aggregate_task` | `DQ_PARTITION_TIMESTAMP` |

У снапшотного DAG'а цепочка трёхзвенная, поэтому строка выглядит так:

```python
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_TIMESTAMP)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_TIMESTAMP)

    # Статистика идёт параллельно DQ и ни на что не влияет: аплоад ждёт таску dq,
    # поэтому падение профилей не блокирует публикацию фич.
    wait_for_sku_price_dag >> aggregate_task >> [dq_task, stats_task]
```

Имена переменных `dq_task` и `stats_task` менять нельзя: на них ассертит wiring-тест.

- [ ] **Step 7: Убедись, что wiring-тест и весь DQ-набор проходят**

Run:
```bash
python3 ci_test/test_feature_stats_task_wiring.py
python3 ci_test/test_dq_task_wiring.py
python3 scripts/validate_dq_configs.py
python3 scripts/validate_ranking_upload_configs.py
```
Expected: все PASS. `test_dq_task_wiring.py` содержит ассерт `assert "dq_task >>" not in text` — новая строка `collect_features >> [dq_task, stats_task]` его не нарушает, но если он покраснел, разберись прежде, чем править тест.

- [ ] **Step 8: Прогони весь набор проверок из AGENTS.md**

Run:
```bash
python3 ci_test/test_script.py
python3 ci_test/test_sync_dbt_sources.py
python3 ci_test/test_sync_iceberg_maintenance.py
python3 scripts/validate_ranking_upload_configs.py
python3 scripts/validate_dq_configs.py
python3 scripts/validate_feature_stats_configs.py
python3 ci_test/test_dq_config.py
python3 ci_test/test_dq_sql.py
python3 ci_test/test_dq_runner.py
python3 ci_test/test_dq_report.py
python3 ci_test/test_dq_results.py
python3 ci_test/test_dq_task.py
python3 ci_test/test_dq_task_wiring.py
python3 ci_test/test_validate_dq_configs.py
python3 ci_test/test_feature_stats_config.py
python3 ci_test/test_feature_stats_sql.py
python3 ci_test/test_feature_stats_runner.py
python3 ci_test/test_feature_stats_results.py
python3 ci_test/test_feature_stats_task.py
python3 ci_test/test_feature_stats_task_wiring.py
python3 ci_test/test_validate_feature_stats_configs.py
git diff --check
```
Expected: все PASS, `git diff --check` без вывода.

- [ ] **Step 9: Коммит**

```bash
git add layers/gold ci_test/test_feature_stats_task_wiring.py
git commit -m "feat(feature_stats): проводка таски в семи DAG'ах источников аплоада"
```

---

### Task 9: Документация

**Files:**
- Create: `feature_stats/README.md`
- Modify: `AGENTS.md` (раздел `## DQ And Source Sync`, строка ~314, и `## Local Validation Commands`, строка ~422)

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: ничего исполняемого.

- [ ] **Step 1: Напиши `feature_stats/README.md`**

Структура по образцу `dq/README.md`. Обязательно раскрой:

- Что считает таска и зачем: профиль распределения каждого числового признака на записанной партиции; DQ отвечает «данные валидны», feature_stats — «данные те же, что вчера».
- Быстрый старт: минимальный блок `feature_stats:` и строка проводки в `dag.py`.
- Полную таблицу ключей конфига с дефолтами (из раздела 6 спеки).
- Правило совпадения partition-настроек с блоком `dq:` и почему CI на этом падает.
- Как определяется набор колонок и что `exclude_columns` с опечаткой роняет таску.
- Снапшотные энтити: `partition_granularity: timestamp`, литерал времени с зоной, `partition_ts` в результатах.
- Таблицу результатов: полный список колонок и смысл `partition_ts` для дневной энтити.
- Известные ограничения: перцентили приближённые (t-digest, ошибка порядка процента на хвостах) — не годятся для порогов вида «ровно p95»; `CAST(... AS DOUBLE)` теряет точность для `BIGINT` за пределами 2^53; набор перцентилей не конфигурируется.
- Локальные проверки: список команд `python3 ci_test/test_feature_stats_*.py` и `python3 scripts/validate_feature_stats_configs.py`.

- [ ] **Step 2: Допиши раздел в `AGENTS.md`**

В `## DQ And Source Sync` добавь после пункта про снапшотные энтити:

```markdown
- Репозиторно-управляемая энтити, чья таблица выгружается в сервис инференса, дополнительно
  получает таску `feature_stats` — она считает профиль распределения каждого числового
  признака записанной партиции и пишет его в `iceberg.silver.feature_platform_feature_stats`.
  Таска идёт параллельно `dq` (`collect_features >> [dq_task, stats_task]`) и ничего не
  блокирует: downstream-сенсоры вешаются только на `dq`, потому что упавший профиль не
  повод не публиковать валидные фичи.
- Блоки `dq:` и `feature_stats:` обязаны совпадать по `partition_column`,
  `partition_granularity`, `partition_date_template` и `snapshot_interval_hours`.
  `scripts/validate_feature_stats_configs.py` падает при расхождении: разные партиции
  у двух тасок одного DAG-рана означают, что профиль посчитан не по тем данным, которые
  проверял DQ, и это не видно ни по падению, ни по пустому результату.
- Набор перцентилей в `feature_stats` зашит в код (`0.05 0.1 0.25 0.5 0.75 0.9 0.95`)
  и колонками `p05..p95` соответствует таблице результатов. Его изменение — это миграция
  таблицы плюс правка `feature_stats/config.py`, а не ключ конфига. Полный контракт —
  в `feature_stats/README.md`.
```

В `## Local Validation Commands` добавь в блок команд:

```bash
python3 scripts/validate_feature_stats_configs.py
python3 ci_test/test_feature_stats_config.py
python3 ci_test/test_feature_stats_sql.py
python3 ci_test/test_feature_stats_runner.py
python3 ci_test/test_feature_stats_results.py
python3 ci_test/test_feature_stats_task.py
python3 ci_test/test_feature_stats_task_wiring.py
python3 ci_test/test_validate_feature_stats_configs.py
```

В `## CI Contracts`, в список того, что делает Drone, добавь строку:

```markdown
- Runs `scripts/validate_feature_stats_configs.py`.
```

И в пункт про migration discovery замени перечисление корней на
`layers/**/config.yaml`, `datasets/**/config.yaml`, `dq/**/config.yaml` и
`feature_stats/**/config.yaml`.

- [ ] **Step 3: Проверь, что документация не разошлась с кодом**

```bash
grep -c "feature_stats" AGENTS.md
python3 scripts/validate_feature_stats_configs.py
```
Expected: `grep` даёт не ноль; валидатор — `Все блоки feature_stats: валидны`.

- [ ] **Step 4: Коммит**

```bash
git add feature_stats/README.md AGENTS.md
git commit -m "docs(feature_stats): README пакета и контракт в AGENTS.md"
```
