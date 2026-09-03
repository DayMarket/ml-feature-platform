from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

BUSINESS_TIMEZONE = ZoneInfo("Asia/Tashkent")


def parse_airflow_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Unsupported partition timestamp for product_metadata: {value!r}"
        )

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(
            f"Unsupported partition timestamp for product_metadata: {value!r}"
        ) from error

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def dt_from_partition_end(partition_end: str) -> datetime:
    local_date = (
        parse_airflow_timestamp(partition_end)
        .astimezone(BUSINESS_TIMEZONE)
        .date()
    )
    return datetime.combine(local_date, time.min)
