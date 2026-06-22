from datetime import date, datetime


def parse_partition_date(value: str) -> date:
    text = value.strip() if isinstance(value, str) else ""
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise ValueError(f"Unsupported partition_start value: {value!r}") from exc
