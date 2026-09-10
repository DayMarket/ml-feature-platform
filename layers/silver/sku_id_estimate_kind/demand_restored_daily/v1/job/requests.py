"""Проверить явные диапазоны одного E3-run."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from .ranges import build_request


def iso_date(value):
    if not isinstance(value, str):
        raise ValueError("Нужна ISO DATE")
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError("Нужна ISO DATE YYYY-MM-DD")
    return result


def prepare_owner_request(config, conf, *, run_id, run_after):
    if not isinstance(conf, dict) or set(conf) - {"selections", "openlineage"}:
        raise ValueError("E3 owner принимает только явные selections")
    if not isinstance(run_after, datetime) or run_after.utcoffset() is None:
        raise ValueError("Нужен aware run_after")
    selections = conf.get("selections")
    if not isinstance(selections, list) or not selections:
        raise ValueError("Нужны явные selections одного E3-run")
    parsed = []
    for item in selections:
        if not isinstance(item, dict) or set(item) != {"run_id", "prediction_date", "start", "end"}:
            raise ValueError("Неверные поля selection")
        value = dict(item)
        for key in ("prediction_date", "start", "end"):
            value[key] = iso_date(item[key])
        if value["prediction_date"] > run_after.astimezone(ZoneInfo("Asia/Tashkent")).date():
            raise ValueError("E3 cutoff в будущем")
        parsed.append(value)
    return build_request(config, copy_id=run_id, selections=parsed)
