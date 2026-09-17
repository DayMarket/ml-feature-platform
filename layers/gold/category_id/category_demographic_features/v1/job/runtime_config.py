from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass(frozen=True)
class SourceSettings:
    product_metadata_table: str
    action_counts_table: str
    demographics_table: str
    business_timezone: str
    lookback_days: int
    min_valid_age: int
    max_valid_age: int

    @property
    def table_names(self) -> tuple[str, ...]:
        return (
            self.product_metadata_table,
            self.action_counts_table,
            self.demographics_table,
        )


def _unquote_scalar(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _read_simple_config(path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {}
    stack = [(-1, config)]

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, separator, value = raw_line.strip().partition(":")
        if not separator or not key:
            continue

        while stack and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        value = value.strip()
        if value:
            parent[key.strip()] = _unquote_scalar(value)
        else:
            nested: dict[str, Any] = {}
            parent[key.strip()] = nested
            stack.append((indent, nested))

    return config


def _required_string(source: dict[str, Any], name: str) -> str:
    value = str(source.get(name, "")).strip()
    if not value:
        raise ValueError(f"source.{name} must be a non-empty string")
    return value


def _required_positive_int(source: dict[str, Any], name: str) -> int:
    value = int(source.get(name, 0))
    if value <= 0:
        raise ValueError(f"source.{name} must be a positive integer")
    return value


def load_source_settings(config_path: Path | None = None) -> SourceSettings:
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / "config.yaml"

    config = _read_simple_config(config_path)
    source = config.get("source")
    if not isinstance(source, dict):
        raise TypeError(f"{config_path}: source must be a mapping")
    if source.get("engine") != "spark_iceberg":
        raise ValueError(f"{config_path}: source.engine must be 'spark_iceberg'")

    business_timezone = _required_string(source, "business_timezone")
    try:
        ZoneInfo(business_timezone)
    except ZoneInfoNotFoundError as error:
        raise ValueError(
            f"source.business_timezone is unknown: {business_timezone!r}"
        ) from error

    min_valid_age = _required_positive_int(source, "min_valid_age")
    max_valid_age = _required_positive_int(source, "max_valid_age")
    if min_valid_age >= max_valid_age:
        raise ValueError("source.min_valid_age must be less than source.max_valid_age")

    return SourceSettings(
        product_metadata_table=_required_string(source, "product_metadata_table"),
        action_counts_table=_required_string(source, "action_counts_table"),
        demographics_table=_required_string(source, "demographics_table"),
        business_timezone=business_timezone,
        lookback_days=_required_positive_int(source, "lookback_days"),
        min_valid_age=min_valid_age,
        max_valid_age=max_valid_age,
    )
