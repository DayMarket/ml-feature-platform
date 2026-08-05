from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceSettings:
    events_table: str
    product_table: str
    category_table: str
    impression_event_type: str
    technical_category_root_id: int
    max_category_depth: int
    window_hours: int

    @property
    def table_names(self) -> tuple[str, ...]:
        return (
            self.events_table,
            self.product_table,
            self.category_table,
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


def load_source_settings(config_path: Path | None = None) -> SourceSettings:
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / "config.yaml"

    config = _read_simple_config(config_path)
    source = config.get("source")
    if not isinstance(source, dict):
        raise TypeError(f"{config_path}: source must be a mapping")
    if source.get("engine") != "spark_iceberg":
        raise ValueError(
            f"{config_path}: source.engine must be 'spark_iceberg'"
        )

    text_fields = (
        "events_table",
        "product_table",
        "category_table",
        "impression_event_type",
    )
    text_values = {}
    for field_name in text_fields:
        value = str(source.get(field_name, "")).strip()
        if not value:
            raise ValueError(
                f"{config_path}: source.{field_name} must be a non-empty string"
            )
        text_values[field_name] = value

    integer_values = {}
    for field_name in (
        "technical_category_root_id",
        "max_category_depth",
        "window_hours",
    ):
        try:
            integer_values[field_name] = int(source[field_name])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"{config_path}: source.{field_name} must be an integer"
            ) from error

    if integer_values["max_category_depth"] < 1:
        raise ValueError(
            f"{config_path}: source.max_category_depth must be positive"
        )
    if integer_values["window_hours"] <= 0:
        raise ValueError(
            f"{config_path}: source.window_hours must be positive"
        )

    return SourceSettings(**text_values, **integer_values)
