import sys
from datetime import date, datetime
from pathlib import Path
from types import ModuleType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

from dq.config import DqConfigError
from dq.task import build_dq_task, build_render_context, parse_partition_value


def _fake_module(monkeypatch, name: str, **attributes) -> None:
    module = ModuleType(name)
    module.__dict__.update(attributes)
    monkeypatch.setitem(sys.modules, name, module)


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
    assert ctx.team == "team:search"


def test_render_context_falls_back_to_default_team() -> None:
    """table.meta.team не обязателен для DQ: без него владельцем считается team:search."""
    config = {
        "table": {
            "catalog": "iceberg",
            "schema": "gold",
            "name": "t",
            "primary_key": "date,product_id",
        }
    }
    assert build_render_context(config, Path("."), date(2026, 8, 19)).team == "team:search"


SNAPSHOT_CONFIG_PATH = (
    "layers/gold/calculated_at_sku_group_id_promotion_id/"
    "dynamic_pricing_sku_group_price_features/v1/config.yaml"
)


def test_render_context_for_snapshot_entity() -> None:
    """Снапшотная энтити проверяет ровно записанный calculated_at, а не весь день."""
    config = yaml.safe_load(Path(SNAPSHOT_CONFIG_PATH).read_text(encoding="utf-8"))
    ctx = build_render_context(config, Path("."), "2026-08-22 06:00:00")
    assert ctx.partition_column == "calculated_at"
    assert ctx.partition_granularity == "timestamp"
    assert ctx.partition_timestamp == datetime(2026, 8, 22, 6, 0, 0)
    assert ctx.partition_date == date(2026, 8, 22)
    assert ctx.snapshot_interval_hours == 3


def test_parse_partition_value_accepts_iso_and_space_separators() -> None:
    assert parse_partition_value("2026-08-22", "date") == (date(2026, 8, 22), None)
    for raw in ("2026-08-22 06:00:00", "2026-08-22T06:00:00", "2026-08-22T06:00:00+00:00"):
        assert parse_partition_value(raw, "timestamp") == (
            date(2026, 8, 22),
            datetime(2026, 8, 22, 6, 0, 0),
        )


def test_parse_partition_value_rejects_date_only_snapshot_template() -> None:
    """Шаблон, отдающий только дату, обязан падать, а не молча брать полночь."""
    try:
        parse_partition_value("2026-08-22", "timestamp")
    except DqConfigError as error:
        assert "partition_date_template" in str(error)
    else:
        raise AssertionError("ожидали DqConfigError для шаблона без времени")


def test_failure_callback_can_be_temporarily_disabled(monkeypatch) -> None:
    options = []

    def decorator(**kwargs):
        options.append(kwargs)
        return lambda function: function

    def unexpected_notification(**kwargs):
        raise AssertionError("send_oncall_notification не должен вызываться")

    _fake_module(monkeypatch, "airflow.providers.trino.hooks.trino", TrinoHook=object)
    _fake_module(monkeypatch, "airflow.sdk", get_current_context=dict, task=decorator)
    _fake_module(
        monkeypatch,
        "airflow_commons.helpers.oncall",
        send_oncall_notification=unexpected_notification,
    )
    build_dq_task(
        "layers/gold/account_id_brand_id/account_brand_features/v1/config.yaml",
        ".",
        failure_callback_enabled=False,
    )
    assert options[-1]["on_failure_callback"] is None


def main() -> int:
    test_render_context_from_entity_config()
    test_render_context_falls_back_to_default_team()
    test_render_context_for_snapshot_entity()
    test_parse_partition_value_accepts_iso_and_space_separators()
    test_parse_partition_value_rejects_date_only_snapshot_template()
    print("DQ task tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
