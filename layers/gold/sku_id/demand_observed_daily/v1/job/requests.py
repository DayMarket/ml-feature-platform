"""Проверить диапазон owner и зафиксировать точные silver-runs."""

from datetime import date, datetime, time, timedelta, timezone

from .planning import checked_references, interval_utc


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


def resolve_references(config, sources, conf, *, run_type, run_id, logical_date, interval_start, interval_end):
    """Scheduled gold 05:00 UTC читает оба silver-run 04:00 UTC того же дня."""
    if conf is None:
        conf = {}
    if not isinstance(conf, dict) or set(sources) != {"sales", "stock"}:
        raise ValueError("Неверный conf или набор источников observed")
    if str(run_type) == "scheduled":
        if conf.get("mode", "regular") != "regular" or "references" in conf:
            raise ValueError("Scheduled observed использует silver-runs своего интервала")
        start, end = interval_utc(interval_start), interval_utc(interval_end)
        if (config["dag"]["schedule"] != "0 5 * * *"
                or any(source["dag"]["schedule"] != "0 4 * * *" for source in sources.values())
                or any(interval_utc(config["dag"]["start_date"]) != interval_utc(source["dag"]["start_date"])
                       for source in sources.values())
                or end - start != timedelta(days=1) or interval_utc(logical_date) != start
                or (start.hour, start.minute, start.second, start.microsecond) != (5, 0, 0, 0)
                or run_id != "scheduled__" + end.isoformat()):
            raise ValueError("Изменился scheduled интервал: пересмотреть привязку silver DQ")
        references = {kind: {"dag_id": source["dag"]["id"],
                      "run_id": "scheduled__" + (end - timedelta(hours=1)).isoformat(),
                      "logical_date": (start - timedelta(hours=1)).isoformat()}
                      for kind, source in sources.items()}
    elif str(run_type) == "manual":
        references = checked_references(conf.get("references"))
    else:
        raise ValueError("Поддерживаются только scheduled regular и manual run")
    if any(ref["dag_id"] != sources[kind]["dag"]["id"] for kind, ref in references.items()):
        raise ValueError("Нужны DQ владельцев sales/stock")
    return references


def owner_arguments(config, conf, *, run_id, run_type, interval_start, interval_end, run_after,
                    references=None, logical_date=None):
    """Ручное окно — start/end либо logical_date; references всегда точные."""
    if conf is None:
        conf = {}
    if not isinstance(conf, dict) or set(conf) - {"mode", "start", "end", "references", "openlineage"}:
        raise ValueError("Неизвестные параметры gold owner run")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Нужен непустой owner run_id")
    if references is not None and "references" in conf:
        raise ValueError("References должны задаваться одним способом")
    refs = checked_references(conf.get("references") if references is None else references)
    now = run_instant(run_after)
    mode = conf.get("mode", "manual" if str(run_type) == "manual" else "regular")
    floor = iso_date(config.get("runtime", {}).get("history_start"), "runtime.history_start")
    if str(run_type) == "manual":
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
    elif str(run_type) == "scheduled":
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
            "interval_end": end.isoformat(), "history_start": first, "references": refs}


def prepare_owner_request(config, repo_root, conf, *, run_id, run_type, interval_start,
                          interval_end, run_after, references=None, catalog=None, query=None, logical_date=None):
    from .orchestration import prepare_request

    arguments = owner_arguments(config, conf, run_id=run_id, run_type=run_type,
        interval_start=interval_start, interval_end=interval_end, run_after=run_after, logical_date=logical_date,
        references=references)
    return prepare_request(config, repo_root, catalog=catalog, query=query, **arguments)
