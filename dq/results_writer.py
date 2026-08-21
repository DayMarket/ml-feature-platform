"""Идемпотентная запись истории DQ-прогонов в Iceberg.

Модуль называется results_writer, а не results, потому что каталог `dq/results/`
хранит config.yaml и миграцию самой таблицы и как namespace-пакет перекрыл бы `dq.results`.
"""

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
    _ = settings  # сигнатура сохранена ради симметрии с write_results
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
                "params": json.dumps(result.params, ensure_ascii=False, default=str),
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
