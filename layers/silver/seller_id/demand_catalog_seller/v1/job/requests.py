"""Зафиксировать полный capture каталога и точную заявку единственному owner."""
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json


def instant(value):
    original = value
    try:
        if isinstance(value, datetime):
            value = datetime.fromisoformat(value.isoformat())
        elif isinstance(value, str) and len(value.strip()) > 10:
            value = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        else:
            raise ValueError("Нет времени")
        if value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Неверный timestamp каталога: {original!r}") from error


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def checked_reference(source, reference):
    if (not isinstance(reference, dict) or set(reference) != {"dag_id", "run_id", "logical_date"}
            or reference.get("dag_id") != source["dag"]["id"]
            or not isinstance(reference.get("run_id"), str) or not reference["run_id"].strip()):
        raise ValueError("Нужны exact dag_id/run_id/logical_date владельца каталога")
    return dict(reference, logical_date=instant(reference["logical_date"]).isoformat())


def capture_request(config, source, conf, *, run_id, run_type, logical_date, interval_start, interval_end):
    run_type = getattr(run_type, "value", run_type)
    if conf is None:
        conf = {}
    allowed = {"mode", "openlineage"} | ({"reference"} if source is not None else set())
    if (not isinstance(conf, dict) or set(conf) - allowed
            or conf.get("mode", "regular") not in {"regular", "manual"}
            or not isinstance(run_id, str) or not run_id.strip()):
        raise ValueError("Каталог принимает только полный срез и известные параметры")
    reference = None
    if run_type == "scheduled":
        start, end = instant(interval_start), instant(interval_end)
        if (conf.get("mode", "regular") != "regular" or "reference" in conf
                or config["dag"]["schedule"] != "0 4 * * *"
                or end - start != timedelta(days=1) or instant(logical_date) != start
                or (start.hour, start.minute, start.second, start.microsecond) != (4, 0, 0, 0)
                or run_id != "scheduled__" + end.isoformat()):
            raise ValueError("Изменился scheduled интервал каталога")
        if source is not None:
            if (source["dag"]["schedule"] != config["dag"]["schedule"]
                    or instant(source["dag"]["start_date"]) != instant(config["dag"]["start_date"])):
                raise ValueError("Изменился source timetable каталога")
            reference = {"dag_id": source["dag"]["id"], "run_id": run_id, "logical_date": start.isoformat()}
    elif run_type == "manual":
        if conf.get("mode") != "manual":
            raise ValueError("Ручной полный срез требует mode=manual")
        if source is not None:
            reference = checked_reference(source, conf.get("reference"))
    else:
        raise ValueError("Поддерживаются только scheduled regular и manual run")
    return {"mode": conf.get("mode", "regular"), "source_manifest_id": run_id,
            "catalog_version": "catalog:" + digest({"dag_id": config["dag"]["id"], "run_id": run_id}),
            "reference": reference}
