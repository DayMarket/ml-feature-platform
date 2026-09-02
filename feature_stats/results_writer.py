"""Идемпотентная запись профилей признаков в Iceberg.

Модуль называется results_writer, а не results, потому что каталог
`feature_stats/results/` хранит config.yaml и миграцию самой таблицы и как
namespace-пакет перекрыл бы `feature_stats.results`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import reduce
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

    table_name обязателен по той же логике, что и остальные три: он входит в
    declared primary_key (`date,partition_ts,dag_id,table_name,feature_name`).
    Сегодня один DAG пишет ровно одну целевую таблицу, поэтому его отсутствие
    здесь ничего не ломает, но это удерживается инвариантом `max_active_runs=1`
    и «одна таска feature_stats на DAG», а не самим фильтром. Если когда-нибудь
    в одном DAG'е появится вторая build_feature_stats_task(...) для другой
    таблицы (снапшотный DAG уже грузит второй entity-конфиг в этом же файле),
    перезапись без table_name молча стёрла бы строки первой таблицы.
    """
    return {
        "date": ctx.render.partition_date,
        "dag_id": meta.dag_id,
        "table_name": ctx.render.table,
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
    # Фильтр собирается из того же словаря, который проверяет тест: перечисление
    # ключей руками означало бы, что правка, роняющая partition_ts, не поймается
    # ничем — pyiceberg в окружении нет, write_results юнит-тестом не покрыть.
    table.overwrite(
        arrow_table,
        overwrite_filter=reduce(And, (EqualTo(key, value) for key, value in values.items())),
    )
