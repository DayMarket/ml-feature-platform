"""Проверить диапазон owner и зафиксировать точный seller-sales run."""

from datetime import date, datetime, time, timedelta, timezone

from .seller_planning import checked_reference, interval_utc


def iso_date(value, name):
    if not isinstance(value, str):
        raise ValueError(f"{name} требует ISO DATE YYYY-MM-DD")
    try:
        result = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{name} требует ISO DATE YYYY-MM-DD") from error
    if result.isoformat() != value:
        raise ValueError(f"{name} требует ISO DATE YYYY-MM-DD")
    return result


def run_instant(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("run_after требует timestamp с timezone")
    return value.astimezone(timezone.utc)


def resolve_reference(config, source, conf, *, run_type, run_id, logical_date, interval_start, interval_end):
    """Scheduled seller-run имеет тот же UTC-интервал; ручной run требует явную ссылку."""
    run_type = getattr(run_type, "value", run_type)
    if conf is None:
        conf = {}
    if not isinstance(conf, dict):
        raise ValueError("Неверный conf SKU-sales")
    if run_type == "scheduled":
        if conf.get("mode", "regular") != "regular" or "reference" in conf:
            raise ValueError("Scheduled SKU-sales использует seller-run своего интервала")
        start, end = interval_utc(interval_start), interval_utc(interval_end)
        if (config["dag"]["schedule"] != "0 4 * * *" or source["dag"]["schedule"] != "0 4 * * *"
                or interval_utc(config["dag"]["start_date"]) != interval_utc(source["dag"]["start_date"])
                or end - start != timedelta(days=1) or interval_utc(logical_date) != start
                or (start.hour, start.minute, start.second, start.microsecond) != (4, 0, 0, 0)
                or run_id != "scheduled__" + end.isoformat()):
            raise ValueError("Изменился scheduled интервал: требуется пересмотреть привязку seller DQ")
        reference = {"dag_id": source["dag"]["id"], "run_id": run_id, "logical_date": start.isoformat()}
    elif run_type == "manual":
        reference = checked_reference(conf.get("reference"))
    else:
        raise ValueError("Поддерживаются только scheduled regular и manual run")
    if reference["dag_id"] != source["dag"]["id"]:
        raise ValueError("Нужен DQ владельца seller-sales")
    return reference


def owner_arguments(config, conf, *, run_id, run_type, interval_start, interval_end, run_after,
                    reference=None, logical_date=None):
    """Ручное окно — start/end либо logical_date; reference всегда точная."""
    run_type = getattr(run_type, "value", run_type)
    if conf is None:
        conf = {}
    if not isinstance(conf, dict) or set(conf) - {"mode", "start", "end", "reference", "openlineage"}:
        raise ValueError("Неизвестные параметры SKU-sales owner run")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Нужен непустой owner run_id")
    if reference is not None and "reference" in conf:
        raise ValueError("References должны задаваться одним способом")
    refs = checked_reference(conf.get("reference") if reference is None else reference)
    now = run_instant(run_after)
    mode = conf.get("mode", "manual" if run_type == "manual" else "regular")
    floor = iso_date(config.get("runtime", {}).get("history_start"), "runtime.history_start")
    if run_type == "manual":
        if mode != "manual":
            raise ValueError("Ручной запуск требует mode=manual")
        if ("start" in conf) != ("end" in conf):
            raise ValueError("start/end задаются только вместе")
        if "start" in conf:
            first = iso_date(conf["start"], "start")
            stop = iso_date(conf["end"], "end")
        else:
            if logical_date is None:
                raise ValueError("Без start/end ручному запуску нужна logical_date")
            refresh = config["runtime"]["refresh_days"]
            if type(refresh) is not int or refresh <= 0:
                raise ValueError("Нужен положительный refresh_days")
            stop = interval_utc(logical_date).date()
            first = max(floor, stop - timedelta(days=refresh))
        start = datetime.combine(first, time.min, tzinfo=timezone.utc)
        end = datetime.combine(stop, time.min, tzinfo=timezone.utc)
        if first < floor:
            raise ValueError("Ручной диапазон начинается раньше доступной истории")
    elif run_type == "scheduled":
        if mode != "regular" or "start" in conf or "end" in conf:
            raise ValueError("Scheduled run не принимает ручные границы")
        first = floor
        start, end = interval_utc(interval_start), interval_utc(interval_end)
        if end - start != timedelta(days=1):
            raise ValueError("Scheduled run требует суточный data interval")
        stop = end.date()
    else:
        raise ValueError("Поддерживаются только scheduled regular и manual run")
    if first >= stop or start >= end or stop > now.date():
        raise ValueError("Нужен непустой диапазон завершённых дней [start,end)")
    return {"run_id": run_id, "mode": mode, "interval_start": start.isoformat(),
            "interval_end": end.isoformat(), "history_start": first, "reference": refs}


def prepare_owner_request(config, repo_root, conf, *, run_id, run_type, interval_start,
                          interval_end, run_after, reference=None, catalog=None, query=None, logical_date=None):
    from .seller_orchestration import prepare_request

    arguments = owner_arguments(config, conf, run_id=run_id, run_type=run_type,
        interval_start=interval_start, interval_end=interval_end, run_after=run_after, logical_date=logical_date,
        reference=reference)
    return prepare_request(config, repo_root, catalog=catalog, query=query, **arguments)
