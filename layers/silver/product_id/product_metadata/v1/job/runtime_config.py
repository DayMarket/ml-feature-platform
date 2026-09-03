from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceSettings:
    product_table: str
    category_table: str
    sku_table: str
    category_gender_table: str
    technical_category_root_id: int
    excluded_brand_id: int

    @property
    def table_names(self) -> tuple[str, ...]:
        return (
            self.product_table,
            self.category_table,
            self.sku_table,
            self.category_gender_table,
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

    table_fields = (
        "product_table",
        "category_table",
        "sku_table",
        "category_gender_table",
    )
    table_values = {}
    for field_name in table_fields:
        value = str(source.get(field_name, "")).strip()
        if not value:
            raise ValueError(
                f"{config_path}: source.{field_name} must be a non-empty string"
            )
        table_values[field_name] = value

    integer_values = {}
    for field_name in (
        "technical_category_root_id",
        "excluded_brand_id",
    ):
        try:
            integer_values[field_name] = int(source[field_name])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"{config_path}: source.{field_name} must be an integer"
            ) from error

    return SourceSettings(**table_values, **integer_values)
