"""Ограничить все задачи owner одним бюджетом от старта DagRun, включая retry."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import threading


def configured_limit(config):
    limit = config.get("runtime", {}).get("run_timeout_seconds")
    if type(limit) is not int or limit <= 0:
        raise ValueError("Нужен положительный run_timeout_seconds")
    return limit


def remaining_seconds(config, context, *, now=None):
    run = context["dag_run"]
    limit = configured_limit(config)
    started = run.start_date
    now = now or datetime.now(timezone.utc)
    if any(not isinstance(v, datetime) or v.utcoffset() is None for v in (started, now)):
        raise ValueError("Бюджет требует timezone-aware start_date и now")
    if started > now:
        raise ValueError("Старт DagRun позже текущего времени")
    remaining = (started + timedelta(seconds=limit) - now).total_seconds()
    if remaining <= 0:
        raise TimeoutError("Исчерпан общий бюджет переноса E3")
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
