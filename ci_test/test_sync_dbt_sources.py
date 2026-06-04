import importlib.util
import tempfile
from pathlib import Path


def load_sync_module():
    module_path = Path("scripts/sync_dbt_sources.py")
    spec = importlib.util.spec_from_file_location("sync_dbt_sources", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    sync = load_sync_module()
    dbt_config = {
        "database_mapping": {"iceberg": "dwh-iceberg"},
        "schema_overrides": {"dev": "staging"},
    }
    table_configs = sync.discover_table_configs(Path("."))
    expected_schemas = {
        str(table_config["schema"])
        for table_config in table_configs
    }
    rendered_schemas = {
        sync._source_schema(dbt_config, table_config, "master")
        for table_config in table_configs
    }
    assert rendered_schemas == expected_schemas

    source_yaml = """version: 2

sources:
  - name: ml_feature_platform_silver
    database: dwh-iceberg
    schema: silver
    tables:
      - name: feature_platform_sku_group_orders
        columns:
          - name: sku_group_id
      - name: feature_platform_sku_group_price_features
        columns:
          - name: sku_group_id
"""
    desired_schemas = {
        "feature_platform_sku_group_orders": "silver",
        "feature_platform_sku_group_price_features": "gold",
    }

    repaired_yaml, removed_tables = sync._remove_misplaced_tables_from_content(
        source_yaml,
        desired_schemas,
    )
    assert removed_tables == [
        (
            "silver",
            "feature_platform_sku_group_price_features",
            "gold",
        )
    ]
    assert sync._extract_source_tables(repaired_yaml) == {
        ("silver", "feature_platform_sku_group_orders")
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        models_path = Path(temp_dir)
        existing_sources_path = models_path / "sources.yaml"
        existing_sources_path.write_text(repaired_yaml, encoding="utf-8")
        source_files = sync._source_files_by_schema(models_path)
        assert sync._sources_path(models_path, "silver", source_files) == existing_sources_path
        assert sync._sources_path(models_path, "gold", source_files) == (
            models_path / "sources_gold.yaml"
        )

    print("dbt source schema sync tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
