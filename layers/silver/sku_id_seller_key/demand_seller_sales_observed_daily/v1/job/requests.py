"""Проверить параметры штатного окна и явного ручного диапазона."""

from datetime import date, datetime, time, timedelta, timezone

from .ranges import interval_utc


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


def owner_arguments(config, conf, *, run_id, run_type, interval_start, interval_end, run_after,
                    logical_date=None):
    """Scheduled обновляет 31 день; manual принимает точный полуоткрытый диапазон."""
    run_type = getattr(run_type, "value", run_type)
    if conf is None:
        conf = {}
    if not isinstance(conf, dict) or set(conf) - {"mode", "start", "end", "openlineage"}:
        raise ValueError("Неизвестные параметры дневного owner run")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Нужен непустой owner run_id")
    now = run_instant(run_after)
    mode = conf.get("mode", "manual" if run_type == "manual" else "regular")
    floor = iso_date(config.get("runtime", {}).get("history_start"), "runtime.history_start")
    if run_type == "scheduled":
        if mode != "regular" or "start" in conf or "end" in conf:
            raise ValueError("Scheduled run не принимает ручные границы")
        start, end = interval_utc(interval_start), interval_utc(interval_end)
        if end - start != timedelta(days=1):
            raise ValueError("Scheduled run требует суточный data interval")
        first, stop = floor, end.date()
    elif run_type == "manual":
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
    else:
        raise ValueError("Поддерживаются только scheduled regular и manual run")
    if first >= stop or start >= end or stop > now.date():
        raise ValueError("Нужен непустой диапазон завершённых дней [start,end)")
    return {"run_id": run_id, "mode": mode, "interval_start": start.isoformat(),
            "interval_end": end.isoformat(), "history_start": first}


def prepare_owner_request(config, repo_root, conf, *, run_id, run_type, interval_start,
                          interval_end, run_after, catalog=None, query=None, logical_date=None):
    from .orchestration import prepare_request

    arguments = owner_arguments(
        config, conf, run_id=run_id, run_type=run_type, interval_start=interval_start,
        interval_end=interval_end, run_after=run_after, logical_date=logical_date,
    )
    return prepare_request(config, repo_root, **arguments, catalog=catalog, query=query)
