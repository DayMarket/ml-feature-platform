import importlib.util
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def load_validator():
    module_path = Path("scripts/validate_dq_configs.py")
    spec = importlib.util.spec_from_file_location("validate_dq_configs", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ENTITY_CONFIG = """table:
  catalog: iceberg
  schema: silver
  name: feature_platform_demo
  primary_key: date,sku_group_id
  meta:
    team: team:search
alerts:
  team: search
  severity: P3
  oncall_webhook_conn_id: oncall_webhook_search
dq:
  tests:
    - name: non_negative
      columns: [orders_cnt]
"""

MIGRATION = """CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'd',
    sku_group_id BIGINT COMMENT 's',
    orders_cnt BIGINT COMMENT 'o'
)
USING iceberg
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
"""


def write_entity(repo: Path, config_text: str, migration_text: str = MIGRATION) -> Path:
    entity = repo / "layers/silver/sku_group_id/demo/v1"
    (entity / "migrations").mkdir(parents=True)
    (entity / "config.yaml").write_text(config_text, encoding="utf-8")
    (entity / "migrations/create_table.sql").write_text(migration_text, encoding="utf-8")
    (entity / "README.md").write_text("# demo\n", encoding="utf-8")
    (repo / "ci_config.yaml").write_text(
        '{"dbt": {"database_mapping": {"iceberg": "dwh-iceberg"}}}', encoding="utf-8"
    )
    return entity / "config.yaml"


def test_valid_config_has_no_problems() -> None:
    validator = load_validator()
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, ENTITY_CONFIG)
        assert validator.validate_config(config_path, repo) == []


def test_unknown_column_is_reported() -> None:
    validator = load_validator()
    bad = ENTITY_CONFIG.replace("orders_cnt", "no_such_column")
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, bad)
        problems = validator.validate_config(config_path, repo)
        assert any("no_such_column" in problem for problem in problems)


def test_unknown_test_name_is_reported() -> None:
    validator = load_validator()
    bad = ENTITY_CONFIG.replace("non_negative", "definitely_not_a_test")
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, bad)
        problems = validator.validate_config(config_path, repo)
        assert any("definitely_not_a_test" in problem for problem in problems)


def test_disabled_dq_without_readme_explanation_is_reported() -> None:
    validator = load_validator()
    disabled = ENTITY_CONFIG.replace("dq:\n  tests:", "dq:\n  enabled: false\n  tests:")
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        config_path = write_entity(repo, disabled)
        problems = validator.validate_config(config_path, repo)
        assert any("README" in problem for problem in problems)


def test_repository_configs_are_all_valid() -> None:
    validator = load_validator()
    repo = Path(".")
    problems: list[str] = []
    for config_path in validator.discover_entity_configs(repo):
        problems.extend(validator.validate_config(config_path, repo))
    assert problems == [], problems


def main() -> int:
    test_valid_config_has_no_problems()
    test_unknown_column_is_reported()
    test_unknown_test_name_is_reported()
    test_disabled_dq_without_readme_explanation_is_reported()
    test_repository_configs_are_all_valid()
    print("DQ config validator tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
