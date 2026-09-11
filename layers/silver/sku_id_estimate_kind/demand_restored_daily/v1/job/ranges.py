"""Перенести точные диапазоны одного E3-run с дневным resume и отдельным DQ допуском."""

from datetime import date, timedelta
from hashlib import sha256
import json
import re

from .checkpoint import resume_day
from .manifest import day_manifest
from .preparation import target_ref
from .query import selection
from .runtime import load_day, preflight_target, read_run

SOURCE_IDENTITY = ("source_run_id", "source_prediction_date", "source_state_version", "output_manifest_sha256")


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def build_request(config, *, copy_id, selections):
    """selections задаёт производитель E3; FP не вычисляет модельное окно зрелости."""
    if not isinstance(copy_id, str) or not copy_id.strip():
        raise ValueError("Нужен идентификатор переноса")
    if not isinstance(selections, (list, tuple)) or not selections:
        raise ValueError("Нужны явные диапазоны E3")
    ranges, dates, previous, identity = [], [], None, None
    for item in selections:
        if not isinstance(item, dict) or set(item) != {"run_id", "prediction_date", "start", "end"}:
            raise ValueError("Неверный состав диапазона")
        selected = selection(**item)
        current = selected["run_id"], selected["prediction_date"]
        if identity is not None and current != identity:
            raise ValueError("Диапазоны должны принадлежать одному E3-run/cutoff")
        identity = current
        start, end = selected["start"], selected["end"]
        if previous is not None and start < previous:
            raise ValueError("Диапазоны должны возрастать без пересечений")
        previous = end
        ranges.append({"start": start.isoformat(), "end": end.isoformat()})
        dates.extend((start + timedelta(days=n)).isoformat() for n in range((end - start).days))
    request = {"copy_id": copy_id, "source_run_id": identity[0], "prediction_date": identity[1].isoformat(),
               "ranges": ranges, "dates": dates, "config_digest": digest(config)}
    return request | {"request_id": digest(request)}


def validate_request(config, request):
    required = {"copy_id", "source_run_id", "prediction_date", "ranges", "dates", "config_digest", "request_id"}
    if not isinstance(request, dict) or set(request) != required:
        raise ValueError("Неверный request переноса E3")
    if not isinstance(request["ranges"], list) or not request["ranges"]:
        raise ValueError("Нет диапазонов запроса")
    selected = []
    for item in request["ranges"]:
        if not isinstance(item, dict) or set(item) != {"start", "end"}:
            raise ValueError("Неверный диапазон запроса")
        try:
            selected.append(selection(run_id=request["source_run_id"],
                                      prediction_date=date.fromisoformat(request["prediction_date"]),
                                      start=date.fromisoformat(item["start"]), end=date.fromisoformat(item["end"])))
        except (TypeError, ValueError) as error:
            raise ValueError("Неверные даты запроса") from error
    if build_request(config, copy_id=request["copy_id"], selections=selected) != request:
        raise ValueError("Запрос либо конфигурация изменены после подготовки")
    return selected


def copy_manifest(request, day):
    return f'e3:{request["request_id"]}:{day.isoformat()}'


def load_range(config, catalog, client, request, *, preflight, require_run_held):
    """Один внешний сериализованный writer; атомарность по дням, не всему диапазону."""
    selected_ranges = validate_request(config, request)
    if not callable(preflight) or not callable(require_run_held):
        raise ValueError("Нужны service preflight и проверка source hold")
    table = preflight_target(config, catalog, request["request_id"])
    if preflight(config, catalog, client) is not True:
        raise ValueError("Service preflight не пройден")
    first = selected_ranges[0]
    passport = read_run(config, client, first)
    entries = []
    for selected in selected_ranges:
        for n in range((selected["end"] - selected["start"]).days):
            day = selected["start"] + timedelta(days=n)
            entries.append((selected, day, day_manifest(config, selected, passport, day)))
    if require_run_held(first, passport) is not True:
        raise ValueError("E3-run не удерживается от очистки")
    table.refresh()
    table_uuid = str(table.metadata.table_uuid)
    head = table.current_snapshot()
    head_id = head.snapshot_id if head else None
    receipts = []
    for selected, day, expected in entries:
        if read_run(config, client, selected) != passport or require_run_held(selected, passport) is not True:
            raise ValueError("Паспорт E3 или hold изменился между днями")
        table.refresh()
        current = table.current_snapshot()
        if (str(table.metadata.table_uuid) != table_uuid
                or (current.snapshot_id if current else None) != head_id):
            raise RuntimeError("Target изменился между днями переноса")
        manifest = copy_manifest(request, day)
        receipt = resume_day(config, catalog, day=day, selected=selected, run=passport,
                             manifest=manifest, version=config["source"]["contract_version"])
        if receipt is None:
            receipt = load_day(config, catalog, client, selected=selected, day=day, manifest=manifest,
                               require_run_held=require_run_held, expected_run=passport)
        if receipt["table_uuid"] != table_uuid or receipt["source_day"] != expected:
            raise ValueError("Дневной receipt не соответствует закреплённому источнику")
        head_id = receipt["snapshot_id"]
        receipts.append(receipt)
    if read_run(config, client, first) != passport or require_run_held(first, passport) is not True:
        raise ValueError("Паспорт E3 или hold изменился после переноса")
    table = catalog.load_table(target_ref(config, catalog.name))
    head = table.current_snapshot()
    if str(table.metadata.table_uuid) != table_uuid or head is None or head.snapshot_id != head_id:
        raise RuntimeError("Target изменился после переноса")
    return {"status": "written", "request_id": request["request_id"], "dates": request["dates"],
            "table_uuid": table_uuid, "snapshot_id": head_id, "day_receipts": receipts,
            "source_identity": {key: entries[0][2][key] for key in SOURCE_IDENTITY}}


def require_range_dq(config, request, written, checks):
    """Сверить доказательства сохранённого DQ каждого дня; сами тесты принадлежат dq/."""
    validate_request(config, request)
    if (not isinstance(written, dict) or written.get("status") != "written"
            or written.get("request_id") != request["request_id"] or written.get("dates") != request["dates"]
            or type(written.get("snapshot_id")) is not int or written["snapshot_id"] <= 0
            or not isinstance(written.get("table_uuid"), str) or not written["table_uuid"]):
        raise ValueError("Неверный receipt диапазона")
    receipts = written.get("day_receipts")
    if (not isinstance(receipts, list) or not isinstance(checks, list)
            or len(receipts) != len(request["dates"]) or len(checks) != len(receipts)):
        raise ValueError("Нет DQ каждого дня диапазона")
    identity = written.get("source_identity")
    if (not isinstance(identity, dict) or set(identity) != set(SOURCE_IDENTITY)
            or identity["source_run_id"] != request["source_run_id"]
            or identity["source_prediction_date"] != request["prediction_date"]
            or type(identity["source_state_version"]) is not int or identity["source_state_version"] <= 0
            or not isinstance(identity["output_manifest_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", identity["output_manifest_sha256"])):
        raise ValueError("Нет единого проверенного источника диапазона")
    for day, receipt, check in zip(request["dates"], receipts, checks, strict=True):
        expected = {"date": day, "table_uuid": written["table_uuid"],
                    "source_manifest_id": copy_manifest(request, date.fromisoformat(day))}
        if (not isinstance(receipt, dict) or receipt.get("status") != "written"
                or type(receipt.get("rows_written")) is not int or receipt["rows_written"] <= 0
                or receipt.get("source_contract_version") != config["source"]["contract_version"]
                or any(receipt.get(k) != v for k, v in expected.items())):
            raise ValueError("Неверный дневной receipt")
        source = receipt.get("source_day")
        if (not isinstance(source, dict) or source.get("date") != day
                or any(source.get(key) != value for key, value in identity.items())
                or type(source.get("rows")) is not int or source["rows"] != receipt["rows_written"]):
            raise ValueError("Неверный дневной источник receipt")
        expected |= {"dq_status": "passed", "snapshot_id": written["snapshot_id"],
                     "request_id": request["request_id"], "rows_checked": receipt["rows_written"],
                     "source_day": source}
        if (not isinstance(check, dict) or type(check.get("rows_checked")) is not int
                or type(check.get("snapshot_id")) is not int
                or any(check.get(k) != v for k, v in expected.items())):
            raise ValueError(f"DQ не подтверждает точную запись за {day}")
    return written | {"status": "ready", "dq_status": "passed"}
