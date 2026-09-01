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
