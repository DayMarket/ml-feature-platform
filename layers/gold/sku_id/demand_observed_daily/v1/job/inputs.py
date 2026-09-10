"""Привязать observed к полному дневному DQ точных sales/stock запусков."""

from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import yaml

from dq.day_range import capture_time, validate_written

from .preparation import metadata, target_ref, validate_source_schema
from .writer import preflight_target


def source_configs(config, repo_root):
    root = Path(repo_root).resolve()
    sources = {}
    for kind in ("sales", "stock"):
        path = (root / config["inputs"][f"{kind}_config"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Input config должен находиться внутри FP")
        source = yaml.safe_load(path.read_text(encoding="utf-8"))
        if (source["table"]["key"] != f"demand_{kind}_daily"
                or source["table"]["primary_key"].replace(" ", "") != "date,sku_id"):
            raise ValueError(f"Неверный владелец/ключ {kind}")
        sources[kind] = source
    return sources


def bind_inputs(sources, references, checked, *, days, captured_at):
    """Caller получает checked из task=dq точного run, без include_prior_dates/latest."""
    if (not isinstance(days, list) or not days or any(type(day) is not date for day in days)
            or days != sorted(set(days))):
        raise ValueError("Нужны возрастающие уникальные DATE gold")
    if not isinstance(captured_at, datetime) or captured_at.utcoffset() is None:
        raise ValueError("Нужно timezone-aware время захвата gold")
    if any(not isinstance(value, dict) or set(value) != {"sales", "stock"}
           for value in (sources, references, checked)):
        raise ValueError("Нужны оба точных входа sales/stock")
    result = {}
    for kind, source in sources.items():
        reference, outcome = references[kind], checked[kind]
        if (not isinstance(reference, dict) or reference.get("dag_id") != source["dag"]["id"]
                or not isinstance(reference.get("run_id"), str) or not reference["run_id"].strip()):
            raise ValueError(f"Неверный upstream DAG/run {kind}")
        if (not isinstance(outcome, dict) or outcome.get("dq_status") != "passed"
                or any(outcome.get(key) != reference[key] for key in ("dag_id", "run_id"))):
            raise ValueError(f"Нет passed DQ точного запуска {kind}")
        written = outcome.get("receipt")
        covered = validate_written(written)
        checks = outcome.get("day_checks")
        if (not isinstance(checks, list) or len(checks) != len(covered)
                or any(not isinstance(check, dict) for check in checks)
                or [check.get("date") for check in checks] != written["dates"]):
            raise ValueError(f"Неполный дневной DQ входа {kind}")
        receipts = {}
        for day, receipt, check in zip(covered, written["day_receipts"], checks, strict=True):
            expected = {"date": day.isoformat(), "dq_status": "passed",
                        "dag_id": reference["dag_id"], "run_id": reference["run_id"],
                        "request_id": written["request_id"], "snapshot_id": written["snapshot_id"],
                        "table_uuid": written["table_uuid"], "source_manifest_id": receipt["source_manifest_id"],
                        "rows_checked": receipt["rows_written"]}
            if any(check.get(key) != value or type(check.get(key)) is not type(value)
                   for key, value in expected.items()):
                raise ValueError(f"DQ не соответствует записи {kind} за {day}")
            if receipt["source_contract_version"] != source["source"]["contract_version"]:
                raise ValueError(f"Другая версия source-контракта {kind}")
            if capture_time(receipt) > captured_at:
                raise ValueError(f"Захват {kind} позже gold")
            receipts[day.isoformat()] = deepcopy(receipt)
        if not set(days).issubset(covered):
            raise ValueError(f"Не все дни gold проверены во входе {kind}")
        result[kind] = {"snapshot_id": written["snapshot_id"], "table_uuid": written["table_uuid"],
                        "dag_id": reference["dag_id"], "run_id": reference["run_id"],
                        "request_id": written["request_id"],
                        "day_receipts": {day.isoformat(): receipts[day.isoformat()] for day in days}}
    metadata(result, "input-preflight", "v1", captured_at)
    return result


def day_inputs(bound, day):
    """Дневной commit не дублирует receipts всего ручного диапазона."""
    if type(day) is not date or set(bound) != {"sales", "stock"}:
        raise ValueError("Нужны DATE и оба входа")
    result = {}
    for kind, source in bound.items():
        receipt = source["day_receipts"].get(day.isoformat())
        if receipt is None:
            raise ValueError(f"Нет проверенного дня {kind}: {day}")
        result[kind] = deepcopy({key: value for key, value in source.items() if key != "day_receipts"})
        result[kind]["day_receipt"] = deepcopy(receipt)
    return result


def preflight_inputs(config, sources, catalog, bound):
    """Все таблицы и точные snapshots должны существовать до чтения данных."""
    if set(sources) != {"sales", "stock"} or set(bound) != set(sources):
        raise ValueError("Нужны оба входа observed")
    tables = {"output": preflight_target(config, catalog)}
    for kind, source in sources.items():
        identifier = target_ref(source, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
        table = catalog.load_table(identifier)
        validate_source_schema(kind, table.schema().as_arrow())
        snapshot_id = bound[kind]["snapshot_id"]
        if (str(table.metadata.table_uuid) != bound[kind]["table_uuid"]
                or type(snapshot_id) is not int or snapshot_id <= 0
                or table.snapshot_by_id(snapshot_id) is None):
            raise ValueError(f"Точный snapshot/UUID {kind} недоступен, latest запрещён")
        tables[kind] = table
    return tables
