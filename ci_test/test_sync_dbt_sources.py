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
    description: "Old description"
    database: dwh-iceberg
    schema: silver
    meta:
      owner: "team:search"
    tables:
      - name: feature_platform_sku_group_orders
        columns:
          - name: sku_group_id
      - name: feature_platform_sku_group_price_features
        columns:
          - name: sku_group_id
      - name: feature_platform_removed_table
        columns:
          - name: sku_group_id
  - name: external_silver
    database: dwh-iceberg
    schema: silver
    tables:
      - name: external_removed_table
        columns:
          - name: id
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
        ),
        (
            "silver",
            "feature_platform_removed_table",
            None,
        )
    ]
    assert sync._extract_source_tables(repaired_yaml) == {
        ("silver", "feature_platform_sku_group_orders"),
        ("silver", "external_removed_table"),
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        models_path = Path(temp_dir)
        sources_path = models_path / sync.SOURCES_FILE_NAME
        repaired_yaml, descriptions_changed = sync._ensure_source_descriptions(
            repaired_yaml
        )
        assert descriptions_changed
        sources_path.write_text(repaired_yaml, encoding="utf-8")

        gold_table_config = {
            "catalog": "iceberg",
            "schema": "gold",
            "name": "feature_platform_sku_group_price_features",
            "primary_key": ["date", "sku_group_id"],
            "team": "team:search",
        }
        gold_source_yaml = sync.render_source_yaml(
            dbt_config,
            gold_table_config,
            "master",
            include_document_header=False,
            include_source_header=True,
        )
        sync._append_to_sources_file(sources_path, gold_source_yaml)

        silver_table_config = {
            "catalog": "iceberg",
            "schema": "silver",
            "name": "feature_platform_sku_group_id_prices",
            "primary_key": ["date", "sku_group_id"],
            "team": "team:search",
        }
        silver_table_yaml = sync.render_source_yaml(
            dbt_config,
            silver_table_config,
            "master",
            include_document_header=False,
            include_source_header=False,
        )
        sync._append_table_to_source_block(
            sources_path,
            "silver",
            silver_table_yaml,
        )

        final_yaml = sources_path.read_text(encoding="utf-8")
        assert sync._extract_source_tables(final_yaml) == {
            ("silver", "feature_platform_sku_group_orders"),
            ("silver", "external_removed_table"),
            ("silver", "feature_platform_sku_group_id_prices"),
            ("gold", "feature_platform_sku_group_price_features"),
        }
        assert (
            'description: "Silver-layer Iceberg tables produced by '
            'ml-feature-platform and consumed by ML feature pipelines."'
        ) in final_yaml
        assert (
            'description: "Gold-layer Iceberg tables produced by '
            'ml-feature-platform and consumed by ML feature pipelines."'
        ) in final_yaml
        assert not (models_path / "sources_gold.yaml").exists()

    print("dbt source schema sync tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
