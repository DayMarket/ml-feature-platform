"""Зафиксировать штатный или ручной диапазон, upstream runs и состояние gold."""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from uuid import UUID

from .inputs import source_configs
from .writer import preflight_target, require_head


def interval_utc(value):
    try:
        if isinstance(value, datetime):
            # Pendulum после astimezone(datetime.timezone.utc) теряет зону при timedelta.
            result = datetime.fromisoformat(value.isoformat())
        elif isinstance(value, str) and len(value.strip()) > 10:
            result = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        else:
            raise ValueError("Нет времени")
        if result.utcoffset() is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Неподдерживаемая граница Airflow: {value!r}") from error


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def checked_references(references):
    if not isinstance(references, dict) or set(references) != {"sales", "stock"}:
        raise ValueError("Нужны точные ссылки обоих silver inputs")
    result = {}
    for kind, ref in references.items():
        if (not isinstance(ref, dict) or set(ref) != {"dag_id", "run_id", "logical_date"}
                or any(not isinstance(ref[k], str) or not ref[k].strip() for k in ("dag_id", "run_id"))):
            raise ValueError("Нужны точные dag_id/run_id/logical_date")
        result[kind] = dict(ref, logical_date=interval_utc(ref["logical_date"]).isoformat())
    return result


def checked_state(state, *, required):
    if state is None and not required:
        return None
    if not isinstance(state, dict) or set(state) != {"table_uuid", "snapshot_id"}:
        raise ValueError("Regular требует исходное состояние gold")
    try:
        UUID(state["table_uuid"])
    except (TypeError, ValueError, AttributeError) as error:
        raise ValueError("Неверный UUID gold") from error
    snap = state["snapshot_id"]
    if snap is not None and (type(snap) is not int or snap <= 0):
        raise ValueError("Неверный snapshot gold")
    return deepcopy(state)


def build_request(config, *, run_id, mode, interval_start, interval_end, history_start,
                  references, output_state=None):
    start, end = interval_utc(interval_start), interval_utc(interval_end)
    if (mode not in ("regular", "manual") or not isinstance(run_id, str) or not run_id.strip()
            or type(history_start) is not date or history_start >= end.date() or start >= end
            or (mode == "manual" and start.date() != history_start)):
        raise ValueError("Неверные run_id/mode или полуоткрытые границы")
    refresh = config["runtime"]["refresh_days"]
    if type(refresh) is not int or refresh <= 0:
        raise ValueError("Неверный refresh_days")
    recent = end.date() - timedelta(days=refresh)
    days = [history_start + timedelta(days=n) for n in range((end.date() - history_start).days)]
    selected = [day.isoformat() for day in days if mode == "manual" or day >= recent]
    request = {"run_id": run_id, "mode": mode, "interval_start": start.isoformat(),
               "interval_end": end.isoformat(), "history_start": history_start.isoformat(),
               "end_exclusive": end.date().isoformat(), "dates": selected,
               "references": checked_references(references),
               "output_state": checked_state(output_state, required=mode == "regular"),
               "config_digest": digest(config)}
    return request | {"request_id": digest(request)}


def validate_request(config, request):
    required = {"run_id", "mode", "interval_start", "interval_end", "history_start", "end_exclusive",
                "dates", "references", "output_state", "config_digest", "request_id"}
    if not isinstance(request, dict) or set(request) != required:
        raise ValueError("Неверная схема gold request")
    if (request["request_id"] != digest({k: v for k, v in request.items() if k != "request_id"})
            or request["config_digest"] != digest(config)):
        raise ValueError("План или конфигурация изменились: требуется новый request")
    try:
        first, stop = date.fromisoformat(request["history_start"]), date.fromisoformat(request["end_exclusive"])
        if not isinstance(request["dates"], list):
            raise ValueError("Нужен список")
        days = [date.fromisoformat(value) for value in request["dates"]]
    except (TypeError, ValueError) as error:
        raise ValueError("Неверные даты gold request") from error
    if (not days or days != sorted(set(days)) or any(not first <= day < stop for day in days)
            or [day.isoformat() for day in days] != request["dates"]):
        raise ValueError("Даты должны быть уникальными ISO DATE внутри истории")
    rebuilt = build_request(config, run_id=request["run_id"], mode=request["mode"],
        interval_start=request["interval_start"], interval_end=request["interval_end"], history_start=first,
        references=request["references"], output_state=request["output_state"])
    if rebuilt != request:
        raise ValueError("В плане отсутствуют обязательные дни или нарушены границы")
    return days


def validate_arguments(config, repo_root, arguments):
    """Проверить параметры и владельцев до открытия connections или чтения coverage."""
    mode = arguments.get("mode")
    sources = source_configs(config, repo_root)
    references = checked_references(arguments.get("references"))
    if any(ref["dag_id"] != sources[kind]["dag"]["id"] for kind, ref in references.items()):
        raise ValueError("Ссылки должны указывать на DQ владельцев silver")
    # Только для чистой валидации параметров, не доказательство существования таблицы.
    validation_state = {"table_uuid": "00000000-0000-0000-0000-000000000000", "snapshot_id": None}
    build_request(config, **arguments, output_state=validation_state if mode == "regular" else None)


def prepare_request(config, repo_root, *, catalog, query, **arguments):
    """Проверить параметры и зафиксировать состояние выхода для regular."""
    validate_arguments(config, repo_root, arguments)
    mode = arguments["mode"]
    if mode == "manual":
        return build_request(config, **arguments)
    table = preflight_target(config, catalog)
    head = table.current_snapshot()
    state = {"table_uuid": str(table.metadata.table_uuid), "snapshot_id": head.snapshot_id if head else None}
    require_head(config, catalog, state["table_uuid"], state["snapshot_id"])
    return build_request(config, **arguments, output_state=state)


def execute_request(config, repo_root, catalog, connection, request, *, fetch_checked, ingested_at=None):
    from .runtime import load_range

    days = validate_request(config, request)
    return load_range(config, repo_root, catalog, connection, days=days, references=request["references"],
        fetch_checked=fetch_checked, request_id=request["request_id"], manifest=f"observed:{request['request_id']}",
        ingested_at=ingested_at, expected_output_state=request["output_state"])
