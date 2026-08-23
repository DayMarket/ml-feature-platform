from datetime import date, datetime, timezone


SUPPORTED_FALLBACK_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d")


def parse_airflow_timestamp(value: str) -> datetime:
    """Разобрать границу интервала Airflow в timezone-aware момент UTC.

    Принимает ISO-строки с зоной и без (`2026-08-20T00:00:00`, `...+00:00`, `...Z`)
    и формат общего Spark-шаблона `YYYY-MM-DD HH:MM:SS`. Наивное значение
    трактуется как UTC: шаблон подставляет `data_interval_*` уже в UTC.
    """
    normalized = value.strip() if isinstance(value, str) else ""
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    parsed = None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for timestamp_format in SUPPORTED_FALLBACK_FORMATS:
            try:
                parsed = datetime.strptime(normalized, timestamp_format)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(
            "Unsupported partition timestamp value for "
            f"buyout_account_history_features: {value!r}. "
            "Expected an ISO datetime or 'YYYY-MM-DD HH:MM:SS'."
        )

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_partition_date(value: str) -> date:
    """Дата партиции D — календарная дата UTC начала интервала Airflow."""
    return parse_airflow_timestamp(value).date()
