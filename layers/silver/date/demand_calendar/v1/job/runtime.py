"""Запустить полную загрузку календаря после preflight источника и служебных таблиц."""

from datetime import datetime, timezone
import logging
from pathlib import Path

import yaml

from .writer import load_calendar

logger = logging.getLogger("airflow.task")


def load_config(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def execute_load(config, repo_root, run_id, mode, *, catalog=None, query_dataframe=None, now=None):
    """Regular и manual одинаково читают весь доступный календарь."""
    if mode not in ("regular", "manual"):
        raise ValueError("Допустимы только regular и manual без ограничения диапазона")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Нужен непустой Airflow run_id")
    if catalog is None:
        from dq.results_writer import load_results_catalog
        catalog = load_results_catalog(config["table"]["catalog"])
    # Все writers используют тот же Hive/S3 каталог, что штатные DQ и stats.
    for relative in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        service = load_config(Path(repo_root) / relative)["table"]
        if service["catalog"] != catalog.name:
            raise ValueError("Служебная таблица использует другой каталог")
        identifier = (service["schema"], service["name"])
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет служебной таблицы {identifier}: сначала применить миграции")
        catalog.load_table(identifier)
    moment = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    result = load_calendar(
        config, catalog, source_manifest_id=run_id, ingested_at=moment,
        query_dataframe=query_dataframe,
    )
    logger.info("Календарь записан: mode=%s, rows=%s, snapshot=%s",
                mode, result["rows_written"], result["snapshot_id"])
    return result
