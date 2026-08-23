import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from validate_feature_stats_configs import discover_entity_configs, validate_config

TABLE_BLOCK = """table:
  catalog: iceberg
  schema: gold
  name: feature_platform_demo
  primary_key: date,sku_group_id
  meta:
    team: team:search
"""

MIGRATION = """CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'd',
    sku_group_id BIGINT COMMENT 'k',
    category_id BIGINT COMMENT 'c',
    conv_imp2order_7 DOUBLE COMMENT 'f'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
"""


def write_entity(root: Path, config_body: str) -> Path:
    entity = root / "layers" / "gold" / "demo" / "v1"
    (entity / "migrations").mkdir(parents=True)
    (entity / "migrations" / "create_table.sql").write_text(MIGRATION, encoding="utf-8")
    config_path = entity / "config.yaml"
    config_path.write_text(TABLE_BLOCK + config_body, encoding="utf-8")
    return config_path


def test_valid_config_has_no_problems() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "feature_stats:\n  exclude_columns:\n    - category_id\n",
        )
        assert validate_config(config_path) == []


def test_exclude_column_absent_from_migrations_is_reported() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "feature_stats:\n  exclude_columns:\n    - categoryy_id\n",
        )
        problems = validate_config(config_path)
        assert len(problems) == 1
        assert "categoryy_id" in problems[0]


def test_partition_settings_diverging_from_dq_are_reported() -> None:
    # Разные партиции у dq и feature_stats в одном DAG-ране — всегда ошибка:
    # профиль считался бы не по тем данным, что проверял DQ.
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "dq:\n  partition_date_template: 'A'\n"
            "feature_stats:\n  partition_date_template: 'B'\n",
        )
        problems = validate_config(config_path)
        assert len(problems) == 1
        assert "partition_date_template" in problems[0]


def test_matching_partition_settings_are_accepted() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(
            Path(tmp),
            "dq:\n  partition_column: date\n  partition_date_template: 'A'\n"
            "feature_stats:\n  partition_column: date\n  partition_date_template: 'A'\n",
        )
        assert validate_config(config_path) == []


def test_invalid_block_is_reported_as_one_problem() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        config_path = write_entity(Path(tmp), "feature_stats:\n  percentiles: [0.5]\n")
        problems = validate_config(config_path)
        assert len(problems) == 1
        assert "percentiles" in problems[0]


def test_config_without_a_table_block_is_skipped() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        entity = Path(tmp) / "layers" / "gold" / "demo" / "v1"
        entity.mkdir(parents=True)
        config_path = entity / "config.yaml"
        config_path.write_text("resources:\n  path: x\n", encoding="utf-8")
        assert validate_config(config_path) == []


def test_repository_configs_are_all_valid() -> None:
    problems: list[str] = []
    for config_path in discover_entity_configs(Path(".")):
        problems.extend(validate_config(config_path))
    assert problems == [], problems


def main() -> int:
    test_valid_config_has_no_problems()
    test_exclude_column_absent_from_migrations_is_reported()
    test_partition_settings_diverging_from_dq_are_reported()
    test_matching_partition_settings_are_accepted()
    test_invalid_block_is_reported_as_one_problem()
    test_config_without_a_table_block_is_skipped()
    test_repository_configs_are_all_valid()
    print("validate_feature_stats_configs tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
