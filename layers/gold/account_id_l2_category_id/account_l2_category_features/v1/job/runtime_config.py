from dataclasses import dataclass
from pathlib import Path
from typing import Any


CONVERSION_LEVELS = (1, 2)
RECENCY_LEVELS = (1, 3, 5)


@dataclass(frozen=True)
class SourceSettings:
    category_level: int
    category_column: str
    product_metadata_table: str
    action_counts_table: str
    impression_counts_table: str | None
    order_items_table: str
    sku_table: str
    business_timezone: str
    event_windows_days: tuple[int, ...]
    order_windows_days: tuple[int, ...]
    successful_order_statuses: tuple[str, ...]

    @property
    def has_impressions(self) -> bool:
        return self.category_level in CONVERSION_LEVELS

    @property
    def has_recency(self) -> bool:
        return self.category_level in RECENCY_LEVELS

    @property
    def table_names(self) -> tuple[str, ...]:
        table_names = [
            self.product_metadata_table,
            self.action_counts_table,
            self.order_items_table,
            self.sku_table,
        ]
        if self.impression_counts_table:
            table_names.append(self.impression_counts_table)
        return tuple(table_names)


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


def _positive_integer_list(source: dict[str, Any], name: str) -> tuple[int, ...]:
    raw_values = str(source.get(name, "")).split(",")
    try:
        values = tuple(int(value.strip()) for value in raw_values if value.strip())
    except ValueError as error:
        raise ValueError(f"source.{name} must contain integers") from error

    if not values or any(value <= 0 for value in values):
        raise ValueError(f"source.{name} must contain positive integers")
    if tuple(sorted(set(values))) != values:
        raise ValueError(f"source.{name} must be sorted and unique")
    return values


def _string_list(source: dict[str, Any], name: str) -> tuple[str, ...]:
    values = tuple(
        value.strip() for value in str(source.get(name, "")).split(",") if value.strip()
    )
    if not values:
        raise ValueError(f"source.{name} must not be empty")
    if len(values) != len(set(values)):
        raise ValueError(f"source.{name} must contain unique values")
    return values


def load_source_settings(config_path: Path | None = None) -> SourceSettings:
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / "config.yaml"

    config = _read_simple_config(config_path)
    source = config.get("source")
    if not isinstance(source, dict):
        raise TypeError(f"{config_path}: source must be a mapping")
    if source.get("engine") != "spark_iceberg":
        raise ValueError(f"{config_path}: source.engine must be 'spark_iceberg'")

    try:
        category_level = int(source["category_level"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("source.category_level must be an integer from 1 to 5") from error
    if category_level not in range(1, 6):
        raise ValueError("source.category_level must be an integer from 1 to 5")

    category_column = _required_string(source, "category_column")
    expected_category_column = f"l{category_level}_category_id"
    if category_column != expected_category_column:
        raise ValueError(
            f"source.category_column must be {expected_category_column!r}"
        )

    impression_counts_table = str(
        source.get("impression_counts_table", "")
    ).strip() or None
    if category_level in CONVERSION_LEVELS and not impression_counts_table:
        raise ValueError(
            "source.impression_counts_table is required for category levels 1 and 2"
        )
    if category_level not in CONVERSION_LEVELS and impression_counts_table:
        raise ValueError(
            "source.impression_counts_table is only allowed for category levels 1 and 2"
        )

    settings = SourceSettings(
        category_level=category_level,
        category_column=category_column,
        product_metadata_table=_required_string(source, "product_metadata_table"),
        action_counts_table=_required_string(source, "action_counts_table"),
        impression_counts_table=impression_counts_table,
        order_items_table=_required_string(source, "order_items_table"),
        sku_table=_required_string(source, "sku_table"),
        business_timezone=_required_string(source, "business_timezone"),
        event_windows_days=_positive_integer_list(source, "event_windows_days"),
        order_windows_days=_positive_integer_list(source, "order_windows_days"),
        successful_order_statuses=_string_list(
            source,
            "successful_order_statuses",
        ),
    )

    if settings.event_windows_days != (3, 7, 14, 28):
        raise ValueError("source.event_windows_days must be 3,7,14,28")
    if settings.order_windows_days != (3, 7, 14, 28, 60, 90):
        raise ValueError("source.order_windows_days must be 3,7,14,28,60,90")
    return settings
