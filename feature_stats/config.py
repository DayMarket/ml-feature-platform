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
        # bool — подкласс int в Python, а PyYAML парсит `yes`/`no` как True/False.
        # Без явной проверки columns_per_query: yes тихо становится int(True) == 1 —
        # один столбец на запрос вместо одного запроса на всю таблицу, то есть 89
        # полных сканов партиции вместо одного на подключённой сейчас самой широкой
        # таблице. int(False) == 0 и без того отбрасывается проверкой <= 0 ниже,
        # но bool здесь запрещается явно и симметрично для обоих значений.
        if isinstance(columns_per_query, bool):
            raise FeatureStatsConfigError(
                f"feature_stats.columns_per_query должен быть целым числом, получено "
                f"булево {columns_per_query!r} — YAML yes/no парсится как bool"
            )
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
