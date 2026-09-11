"""Ограничить все задачи owner одним бюджетом от старта DagRun, включая retry."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import threading


def configured_limits(config):
    limits = config.get("runtime", {}).get("run_timeout_seconds")
    if (not isinstance(limits, dict) or set(limits) != {"regular", "manual"}
            or any(type(v) is not int or v <= 0 for v in limits.values())
            or limits["manual"] < limits["regular"]):
        raise ValueError("Нужны положительные фиксированные run_timeout_seconds regular/manual")
    return limits


def remaining_seconds(config, context, *, now=None):
    run = context["dag_run"]
    conf = run.conf or {}
    if not isinstance(conf, dict):
        raise ValueError("Неверный conf запуска")
    default_mode = "manual" if str(getattr(run, "run_type", "scheduled")) == "manual" else "regular"
    mode = conf.get("mode", default_mode)
    limits = configured_limits(config)
    if mode not in limits:
        raise ValueError("Неизвестный режим бюджета")
    started = run.start_date
    now = now or datetime.now(timezone.utc)
    if any(not isinstance(v, datetime) or v.utcoffset() is None for v in (started, now)):
        raise ValueError("Бюджет требует timezone-aware start_date и now")
    if started > now:
        raise ValueError("Старт DagRun позже текущего времени")
    remaining = (started + timedelta(seconds=limits[mode]) - now).total_seconds()
    if remaining <= 0:
        raise TimeoutError(f"Исчерпан общий бюджет {mode} запуска")
    return remaining


@contextmanager
def run_guard(config, context):
    # Airflow POSIX timeout вне главного потока только предупреждает, но не прерывает работу.
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("Ограничение времени задачи требует главного потока")
    from airflow.sdk.execution_time.timeout import timeout

    seconds = remaining_seconds(config, context)
    with timeout(seconds=seconds, error_message="Исчерпан общий бюджет дневного owner"):
        yield
