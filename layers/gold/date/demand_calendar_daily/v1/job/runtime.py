"""Связать gold-календарь с успешным DQ двух согласованных silver-срезов."""

from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path

import yaml

from .preparation import prepare, target_ref, validate_schema
from .writer import write_prepared

logger = logging.getLogger("airflow.task")


def utc_timestamp(value):
    original = value
    try:
        if isinstance(value, str) and len(value.strip()) > 10:
            value = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise ValueError("Нужен timestamp")
        value = datetime.fromisoformat(value.isoformat())
        return value.replace(tzinfo=timezone.utc) if value.utcoffset() is None else value.astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError(f"Неверный Airflow timestamp: {original!r}") from exc


def source_configs(config, repo_root):
    root = Path(repo_root).resolve()
    result = {}
    for name, key in (("calendar", "demand_calendar"), ("events", "demand_event_calendar")):
        path = (root / config["inputs"][name + "_config"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Input config должен находиться внутри FP")
        source = yaml.safe_load(path.read_text(encoding="utf-8"))
        if source["table"]["key"] != key:
            raise ValueError(f"Неверный владелец {name}")
        result[name] = source
    event_calendar = (root / result["events"]["inputs"]["calendar_config"]).resolve()
    if event_calendar != (root / config["inputs"]["calendar_config"]).resolve():
        raise ValueError("Gold и silver events используют разные календари")
    return result


def validate_references(references, sources):
    if not isinstance(references, dict) or set(references) != {"calendar", "events"}:
        raise ValueError("Нужны точные ссылки calendar и events")
    result = {}
    for name, source in sources.items():
        ref = references[name]
        if not isinstance(ref, dict) or set(ref) != {"dag_id", "run_id", "logical_date"}:
            raise ValueError("Нужны dag_id/run_id/logical_date")
        if ref["dag_id"] != source["dag"]["id"] or not isinstance(ref["run_id"], str) or not ref["run_id"].strip():
            raise ValueError("Неверный upstream DAG/run")
        result[name] = {**ref, "logical_date": utc_timestamp(ref["logical_date"]).isoformat()}
    return result


def scheduled_references(config, sources, interval_start, interval_end):
    if (config["dag"]["schedule"], sources["calendar"]["dag"]["schedule"],
            sources["events"]["dag"]["schedule"]) != ("0 4 * * *", "0 3 * * *", "10 3 * * *"):
        raise ValueError("Расписание изменилось: пересмотреть привязку silver DQ")
    start, end = utc_timestamp(interval_start), utc_timestamp(interval_end)
    if end - start != timedelta(days=1) or (start.hour, start.minute, start.second, start.microsecond) != (4, 0, 0, 0):
        raise ValueError("Нужен полный scheduled интервал 04:00 UTC")
    return {name: {"dag_id": sources[name]["dag"]["id"],
                   "run_id": "scheduled__" + (end - timedelta(minutes=delta)).isoformat(),
                   "logical_date": (start - timedelta(minutes=delta)).isoformat()}
            for name, delta in (("calendar", 60), ("events", 50))}


def checked_receipt(checked, reference):
    if not isinstance(checked, dict) or checked.get("dq_status") != "passed":
        raise ValueError("Нет успешного upstream DQ, latest запрещён")
    if (checked.get("dag_id"), checked.get("run_id")) != (reference["dag_id"], reference["run_id"]):
        raise ValueError("DQ относится к другому запуску")
    receipt = checked.get("receipt")
    if not isinstance(receipt, dict) or receipt.get("status") != "written" or receipt.get("source_manifest_id") != reference["run_id"]:
        raise ValueError("Неверный writer receipt в DQ")
    return receipt


def read_snapshot(table, receipt):
    snapshot_id = receipt.get("snapshot_id")
    if type(snapshot_id) is not int or snapshot_id <= 0 or table.snapshot_by_id(snapshot_id) is None:
        raise ValueError("Точный upstream snapshot недоступен")
    if str(table.metadata.table_uuid) != receipt.get("table_uuid"):
        raise ValueError("Upstream UUID не совпал")
    moment = datetime.fromisoformat(receipt["ingested_at"])
    if moment.utcoffset() is None:
        raise ValueError("Upstream capture без зоны")
    batch = table.scan(snapshot_id=snapshot_id).to_arrow()
    if not batch.num_rows or type(receipt.get("rows_written")) is not int or batch.num_rows != receipt["rows_written"]:
        raise ValueError("Число upstream строк не совпало")
    if set(batch["source_manifest_id"].to_pylist()) != {receipt["source_manifest_id"]}:
        raise ValueError("Upstream manifest не совпал")
    for value in batch["ingested_at"].to_pylist():
        if not isinstance(value, datetime) or utc_timestamp(value) != moment:
            raise ValueError("Upstream время захвата не совпало")
    days = batch["date"].to_pylist()
    if any(day is None for day in days) or (min(days).isoformat(), max(days).isoformat()) != (receipt.get("date_min"), receipt.get("date_max")):
        raise ValueError("Диапазон upstream дат не совпал")
    return batch


def execute_load(config, repo_root, run_id, mode, references, checked, *, catalog=None, now=None):
    if mode not in ("regular", "manual"):
        raise ValueError("Неизвестный режим gold")
    sources = source_configs(config, repo_root)
    references = validate_references(references, sources)
    receipts = {name: checked_receipt(checked[name], references[name]) for name in sources}
    captured = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    if not isinstance(captured, datetime) or captured.utcoffset() is None:
        raise ValueError("Нужен aware ingested_at gold")
    # Старый успешный DQ не делает сегодняшний gold свежим автоматически.
    for receipt in receipts.values():
        source_time = datetime.fromisoformat(receipt["ingested_at"])
        if source_time.utcoffset() is None or not timedelta(0) <= captured - source_time <= timedelta(days=2):
            raise ValueError("Upstream захват устарел либо находится в будущем")
    embedded = receipts["events"].get("coverage_report", {}).get("calendar_source", {})
    if embedded.get("receipt") != receipts["calendar"]:
        raise ValueError("Events собраны по другой версии календаря")
    if catalog is None:
        from dq.results_writer import load_results_catalog
        catalog = load_results_catalog(config["table"]["catalog"])
    if embedded.get("catalog") != catalog.name or embedded.get("identifier") != list(target_ref(sources["calendar"], catalog.name)):
        raise ValueError("Events ссылаются на другую таблицу календаря")
    configs = {**sources, "output": config}
    for name, path in (("dq", "dq/results/config.yaml"), ("stats", "feature_stats/results/config.yaml")):
        configs[name] = yaml.safe_load((Path(repo_root) / path).read_text())
    tables = {}
    # Все входные, выходная и служебные таблицы проверяются до чтения snapshot.
    for name, cfg in configs.items():
        identifier = target_ref(cfg, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет таблицы {identifier}: сначала применить миграции")
        tables[name] = catalog.load_table(identifier)
    schema = tables["output"].schema().as_arrow()
    validate_schema(schema)
    inputs = {name: read_snapshot(tables[name], receipts[name]) for name in sources}
    batch = prepare(inputs["calendar"], inputs["events"], schema,
                    calendar_receipt=receipts["calendar"], events_receipt=receipts["events"],
                    run_id=run_id, ingested_at=captured)
    result = write_prepared(config, catalog, batch)
    result["upstream_dq"] = references
    logger.info("Gold calendar: mode=%s, rows=%s, snapshot=%s", mode, result["rows_written"], result["snapshot_id"])
    return result
