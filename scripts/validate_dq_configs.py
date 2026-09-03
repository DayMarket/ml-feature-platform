#!/usr/bin/env python3
"""CI-гейт: проверяет корректность блоков dq: во всех энтити-конфигах."""

from __future__ import annotations

import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import DqConfigError, DqSettings, RenderContext, load_dq_settings  # noqa: E402
from dq.tests import (  # noqa: E402
    partition_expression,
    partition_literal,
    render,
    table_ref,
)

ENTITY_CONFIG_ROOTS = ("layers", "datasets")
# Идентификаторы в DDL бывают в обратных кавычках: `position` — зарезервированное
# слово Spark SQL. Без них колонка не попадает в известные и даёт ложную тревогу.
COLUMN_DEFINITION = re.compile(
    r"^\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+[A-Za-z]", re.MULTILINE
)
ADD_COLUMN = re.compile(
    r"ADD\s+COLUMNS?\s*\(?\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE
)
QUOTED_IDENTIFIER = re.compile(r'"((?:[^"]|"")*)"')
# Колонки чужих таблиц: их нет и не должно быть в миграциях этой энтити.
FOREIGN_COLUMN_PARAMS = ("to_column", "reference_date_column")
# Дата и снапшот, от которых рендерится SQL при проверке. Конкретные значения
# ни на что не влияют: из отрендеренной строки берутся только идентификаторы.
PROBE_DATE = date(2026, 1, 2)
PROBE_TIMESTAMP = datetime(2026, 1, 2, 12, 0, 0)


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

    for source, column in referenced_columns(settings, config):
        if column.lower() not in known_columns:
            problems.append(
                f"{config_path}: {source} рендерит SQL по колонке {column!r}, "
                f"которой нет в миграциях энтити"
            )
    return problems


def probe_context(settings: DqSettings, config: dict[str, Any]) -> RenderContext:
    """Контекст рендера с любой валидной датой: из SQL берутся только идентификаторы."""
    table = config.get("table") or {}
    snapshot = settings.partition_granularity == "timestamp"
    return RenderContext(
        # Каталог не резолвится через ci_config.yaml намеренно: маппинг каталогов
        # к именам колонок отношения не имеет, а его отсутствие в тестовом репозитории
        # превратило бы проверку колонок в проверку ci_config.yaml.
        catalog_alias=str(table.get("catalog", "iceberg")),
        schema=str(table.get("schema", "")),
        table=str(table.get("name", "")),
        primary_key=tuple(
            column.strip() for column in str(table.get("primary_key", "")).split(",") if column.strip()
        ),
        partition_column=settings.partition_column,
        partition_date=PROBE_DATE,
        scope=settings.scope,
        sample_rows=settings.sample_rows,
        partition_granularity=settings.partition_granularity,
        partition_timestamp=PROBE_TIMESTAMP if snapshot else None,
        snapshot_interval_hours=settings.snapshot_interval_hours,
    )


def referenced_columns(settings: DqSettings, config: dict[str, Any]) -> list[tuple[str, str]]:
    """Колонки, которые DQ действительно рендерит, — из самого отрендеренного SQL.

    Проверять параметры тестов по списку недостаточно: колонка партиции приходит из
    `dq.partition_column`, ключ — из `table.primary_key` через проекцию sample, и обе
    попадают в SQL в обход `tests[].columns`. Извлечение идентификаторов из готовой
    строки не может разъехаться с рендерерами: это тот же SQL, что уйдёт в Trino.
    Не покрыты только сырые выражения `expression_is_true` — они не квотируются
    (их прогон на живом Trino описан в AGENTS.md).
    """
    ctx = probe_context(settings, config)
    statements: list[tuple[str, str, tuple[str, ...]]] = []

    if settings.warmup_days > 0 and ctx.scope != "table":
        partition_expr = partition_expression(ctx)
        statements.append(
            (
                "dq.warmup_days",
                f"SELECT COUNT(DISTINCT {partition_expr}) FROM {table_ref(ctx)} "
                f"WHERE {partition_expr} < {partition_literal(ctx)}",
                (),
            )
        )

    for spec in settings.tests:
        rendered = render(spec, ctx)
        foreign = tuple(
            str(spec.params[key]) for key in FOREIGN_COLUMN_PARAMS if spec.params.get(key)
        )
        statements.append((f"тест {spec.name}", rendered.sql, foreign))
        if rendered.sample_sql:
            statements.append((f"sample теста {spec.name}", rendered.sample_sql, foreign))

    own_names = {ctx.catalog_alias, ctx.schema, ctx.table}
    referenced: list[tuple[str, str]] = []
    seen: set[str] = set()
    for source, sql, foreign in statements:
        for match in QUOTED_IDENTIFIER.findall(sql):
            column = match.replace('""', '"')
            if column in own_names or column in foreign or column in seen:
                continue
            seen.add(column)
            referenced.append((source, column))
    return referenced


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
