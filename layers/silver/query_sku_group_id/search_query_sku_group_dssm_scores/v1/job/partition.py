from datetime import datetime, timedelta, timezone


def parse_airflow_timestamp(value: str) -> datetime:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = None

    if parsed is None:
        for timestamp_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z"):
            try:
                parsed = datetime.strptime(text, timestamp_format)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(
            f"Unsupported partition timestamp for search_query_sku_group_dssm_scores: {value!r}"
        )

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_day_bounds_from_interval_start(value: str) -> tuple[datetime, datetime]:
    interval_start = parse_airflow_timestamp(value)
    day_start = datetime.combine(
        interval_start.date(),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    return day_start, day_start + timedelta(days=1)
