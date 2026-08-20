import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

from dq.task import build_render_context


def test_render_context_from_entity_config() -> None:
    config = yaml.safe_load(
        Path("layers/silver/sku_group_id/sku_group_id_prices/v1/config.yaml").read_text(encoding="utf-8")
    )
    ctx = build_render_context(config, Path("."), date(2026, 8, 19))
    assert ctx.catalog_alias == "dwh-iceberg"
    assert ctx.schema == "silver"
    assert ctx.table == "feature_platform_sku_group_id_prices"
    assert ctx.primary_key == ("date", "sku_group_id")
    assert ctx.partition_column == "date"
    assert ctx.partition_date == date(2026, 8, 19)
    assert ctx.scope == "partition"


def main() -> int:
    test_render_context_from_entity_config()
    print("DQ task tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
