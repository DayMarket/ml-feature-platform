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
        # render_stats_query фильтрует по partition_column в каждом запуске, но
        # в exclude_columns она не перечислена и проверкой ниже не покрывается.
        if settings.partition_column.lower() not in known_columns:
            problems.append(
                f"{config_path}: feature_stats.partition_column — {settings.partition_column!r}, "
                "но такой колонки нет в миграциях энтити; render_stats_query фильтрует по ней "
                "в каждом запуске"
            )
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
