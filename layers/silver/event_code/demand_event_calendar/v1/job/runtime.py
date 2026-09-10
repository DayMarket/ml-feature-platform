"""Связать полный захват событий с успешным DQ конкретного календарного запуска."""

from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path

import yaml

from .extraction import _calendar_config, target_ref
from .writer import load_events

logger = logging.getLogger("airflow.task")


def utc_timestamp(value):
    """Прочитать Airflow ISO/space timestamp; наивные границы DAG имеют зону UTC."""
    try:
        if isinstance(value, str):
            if len(value.strip()) <= 10:
                raise ValueError("Нет времени")
            value = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise ValueError("Нужен timestamp")
        # Не сохранять subclass Pendulum: арифметика должна оставаться в aware datetime.
        value = datetime.fromisoformat(value.isoformat())
        return value.replace(tzinfo=timezone.utc) if value.utcoffset() is None else value.astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError(f"Неверный Airflow timestamp: {value!r}") from exc


def scheduled_calendar_reference(config, calendar_config, interval_start, interval_end):
    """Связать cron 03:10 с календарём 03:00, оба с явным DataIntervalTimetable."""
    if (config["dag"]["schedule"], calendar_config["dag"]["schedule"]) != ("10 3 * * *", "0 3 * * *"):
        raise ValueError("Расписания изменились: пересмотреть привязку к календарному DQ")
    start, end = utc_timestamp(interval_start), utc_timestamp(interval_end)
    if end - start != timedelta(days=1) or (start.hour, start.minute, start.second, start.microsecond) != (3, 10, 0, 0):
        raise ValueError("Нужен полный scheduled data interval 03:10 UTC")
    cal_start, cal_end = start - timedelta(minutes=10), end - timedelta(minutes=10)
    # В Airflow 3.1.8 scheduled run_id содержит run_after (конец интервала), не logical_date.
    return {"dag_id": calendar_config["dag"]["id"],
            "run_id": "scheduled__" + cal_end.isoformat(), "logical_date": cal_start.isoformat()}


def validate_reference(reference, calendar_config):
    if not isinstance(reference, dict) or set(reference) != {"dag_id", "run_id", "logical_date"}:
        raise ValueError("Нужны точные dag_id/run_id/logical_date календаря")
    if reference["dag_id"] != calendar_config["dag"]["id"]:
        raise ValueError("Ссылка указывает не на DAG владельца календаря")
    if not isinstance(reference["run_id"], str) or not reference["run_id"].strip():
        raise ValueError("Нет upstream run_id")
    return {**reference, "logical_date": utc_timestamp(reference["logical_date"]).isoformat()}


def checked_calendar_receipt(checked, reference):
    """Written XCom не заменяет успешный DQ того же запуска."""
    if not isinstance(checked, dict) or checked.get("dq_status") != "passed":
        raise ValueError("Нет успешного DQ receipt календаря; latest не используется")
    if (checked.get("dag_id"), checked.get("run_id")) != (reference["dag_id"], reference["run_id"]):
        raise ValueError("DQ receipt относится к другому запуску календаря")
    receipt = checked.get("receipt")
    if not isinstance(receipt, dict) or receipt.get("status") != "written":
        raise ValueError("DQ не содержит receipt записи календаря")
    if receipt.get("source_manifest_id") != reference["run_id"]:
        raise ValueError("Calendar manifest не совпадает с upstream run_id")
    return receipt


def execute_load(config, repo_root, run_id, mode, reference, checked, *,
                 catalog=None, query_records=None, now=None):
    """Проверить upstream DQ и служебные таблицы для regular/manual."""
    if mode not in ("regular", "manual"):
        raise ValueError("Неверный режим полной загрузки событий")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Нужен run_id событий")
    reference = validate_reference(reference, _calendar_config(config, repo_root))
    receipt = checked_calendar_receipt(checked, reference)
    if catalog is None:
        from dq.results_writer import load_results_catalog
        catalog = load_results_catalog(config["table"]["catalog"])
    for relative in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        service = yaml.safe_load((Path(repo_root) / relative).read_text(encoding="utf-8"))
        identifier = target_ref(service, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет служебной таблицы {identifier}: сначала применить миграции")
        catalog.load_table(identifier)
    captured = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    result = load_events(config, catalog, repo_root, calendar_receipt=receipt,
                         source_manifest_id=run_id, ingested_at=captured,
                         query_records=query_records)
    result["upstream_dq"] = {"dag_id": reference["dag_id"], "run_id": reference["run_id"]}
    logger.info("События записаны: mode=%s, rows=%s, snapshot=%s",
                mode, result["rows_written"], result["snapshot_id"])
    return result
