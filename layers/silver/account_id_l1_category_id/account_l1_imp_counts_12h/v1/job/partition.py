from datetime import datetime, timezone


def parse_airflow_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "Unsupported partition timestamp for "
            f"account_l1_imp_counts_12h: {value!r}"
        )

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(
            "Unsupported partition timestamp for "
            f"account_l1_imp_counts_12h: {value!r}"
        ) from error

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
