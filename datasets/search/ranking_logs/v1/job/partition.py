from datetime import date, datetime, timezone
from typing import Tuple


def parse_airflow_timestamp(value: str) -> datetime:
    """Разбирает Airflow-таймстемп в aware UTC datetime."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    for candidate in (normalized, normalized.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    raise ValueError(
        "Unsupported partition timestamp format. "
        f"Expected Airflow ISO timestamp or YYYY-MM-DD HH:MM:SS, got {value!r}"
    )


def collection_date(partition_end: str) -> date:
    """Дата фактического запуска DAG'а: конец недельного интервала."""
    return parse_airflow_timestamp(partition_end).date()


def event_date_bounds(partition_start: str, partition_end: str) -> Tuple[date, date]:
    """Границы окна логов: начало включительно, конец исключительно.

    Недельный data_interval начинается и заканчивается в один и тот же час, поэтому
    календарные сутки окна — от даты его начала до даты его конца, не включая
    последнюю: она ещё не закрыта на момент запуска.
    """
    start = parse_airflow_timestamp(partition_start).date()
    end = parse_airflow_timestamp(partition_end).date()
    if end <= start:
        raise ValueError(
            f"partition_end must be after partition_start, got {partition_start!r} and {partition_end!r}"
        )
    return start, end
