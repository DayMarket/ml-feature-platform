from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments


def _read_simple_config(path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, config)]
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
        if value.strip():
            parent[key.strip()] = value.strip()
        else:
            nested: dict[str, Any] = {}
            parent[key.strip()] = nested
            stack.append((indent, nested))
    return config


def _entity_config() -> dict[str, Any]:
    entity_dir = Path(__file__).resolve().parents[1]
    return _read_simple_config(entity_dir / "config.yaml")


def _parse_snapshot_timestamp(value: str) -> datetime:
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
            f"Unsupported snapshot timestamp: {value!r}. "
            "Expected an ISO datetime with timezone or 'YYYY-MM-DD HH:MM:SS'."
        )

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _timestamp_literal(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def build_dynamic_pricing_sku_group_price_features(
    spark: SparkSession,
    calculated_at: datetime,
    source_table: str,
) -> DataFrame:
    calculated_at_literal = _timestamp_literal(calculated_at)
    return spark.sql(
        f"""
SELECT
    TIMESTAMP '{calculated_at_literal}' AS calculated_at,
    CAST(sku_group_id AS BIGINT) AS sku_group_id,
    promotion_id,
    MIN(sell_price) AS min_sell_price,
    MAX(sell_price) AS max_sell_price,
    AVG(sell_price) AS avg_sell_price,
    MIN(discount) AS min_discount,
    MAX(discount) AS max_discount,
    AVG(discount) AS avg_discount,
    MIN(discount_fraction) AS min_discount_fraction,
    MAX(discount_fraction) AS max_discount_fraction,
    AVG(discount_fraction) AS avg_discount_fraction
FROM {source_table}
WHERE calculated_at = TIMESTAMP '{calculated_at_literal}'
  AND sku_group_id IS NOT NULL
  AND promotion_id IS NOT NULL
GROUP BY
    CAST(sku_group_id AS BIGINT),
    promotion_id
"""
    )


def save_dynamic_pricing_sku_group_price_features(
    spark: SparkSession,
    calculated_at: datetime,
    source_table: str,
    target_table: str,
) -> None:
    features = build_dynamic_pricing_sku_group_price_features(
        spark=spark,
        calculated_at=calculated_at,
        source_table=source_table,
    )
    calculated_at_literal = _timestamp_literal(calculated_at)
    features.writeTo(target_table).overwrite(
        F.col("calculated_at") == F.expr(f"TIMESTAMP '{calculated_at_literal}'")
    )


def run(spark: SparkSession, arguments: Arguments) -> None:
    config = _entity_config()
    calculated_at = _parse_snapshot_timestamp(arguments.partition_end)
    save_dynamic_pricing_sku_group_price_features(
        spark=spark,
        calculated_at=calculated_at,
        source_table=str(config["source"]["sku_price_table"]),
        target_table=arguments.table_name,
    )
