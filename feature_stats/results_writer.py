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
