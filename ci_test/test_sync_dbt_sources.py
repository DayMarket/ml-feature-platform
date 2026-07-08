import base64
import importlib.util
import json
import subprocess
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
    dataset_table = next(
        table_config
        for table_config in table_configs
        if table_config["config_path"]
        == "datasets/search/search_ranking/v1/config.yaml"
    )
    assert dataset_table["name"] == "feature_platform_dataset_search_ranking_v1"
    assert dataset_table["schema"] == "silver"
    assert dataset_table["create_dbt_pr"] is True
    product_queries_table = next(
        table_config
        for table_config in table_configs
        if table_config["config_path"]
        == "layers/silver/product_id/product_search_queries/v1/config.yaml"
    )
    assert isinstance(product_queries_table["create_dbt_pr"], bool)
    assert sync._parse_bool_flag(
        {"create_dbt_pr": "false"},
        "create_dbt_pr",
        Path("config.yaml"),
    ) is False
    expected_schemas = {
        str(table_config["schema"])
        for table_config in table_configs
    }
    rendered_schemas = {
        sync._source_schema(dbt_config, table_config, "master")
        for table_config in table_configs
    }
    assert rendered_schemas == expected_schemas

    open_pr_source_yaml = """version: 2

sources:
  - name: ml_feature_platform_gold
    database: dwh-iceberg
    schema: gold
    tables:
      - name: feature_platform_table_from_open_pr
        columns:
          - name: date
"""
    encoded_open_pr_source_yaml = base64.b64encode(
        open_pr_source_yaml.encode("utf-8")
    ).decode("ascii")
    original_run = sync._run
    original_open_pr_branch_tables = sync._open_pr_branch_tables
    try:
        sync._open_pr_branch_tables = lambda runtime, pr_number, models_path: set()

        def fake_run(command, **kwargs):
            if command[:3] == ["gh", "pr", "list"]:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout=json.dumps(
                        [
                            {
                                "number": 123,
                                "url": "https://github.com/DayMarket/dbt-trino/pull/123",
                                "title": "Add ml-feature-platform dbt sources",
                                "headRefName": "automation/source-sync",
                                "headRefOid": "abc123",
                            }
                        ]
                    ),
                    stderr="",
                )
            if command[:2] == ["gh", "api"]:
                assert command[2] == (
                    "repos/DayMarket/dbt-trino/contents/"
                    "models/ml_feature_platform/sources.yaml?ref=abc123"
                )
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout=json.dumps(
                        {
                            "content": encoded_open_pr_source_yaml,
                            "encoding": "base64",
                        }
                    ),
                    stderr="",
                )
            raise AssertionError(f"unexpected command: {command}")

        sync._run = fake_run
        runtime = sync.RuntimeConfig(
            branch="master",
            dbt_repo_url="https://github.com/DayMarket/dbt-trino.git",
            git_token="token",
            workspace=Path("."),
            dry_run=True,
            build_url="",
        )
        assert sync._open_pr_tables(
            runtime,
            "DayMarket/dbt-trino",
            {"models_path": "models/ml_feature_platform"},
        ) == {
            (
                "gold",
                "feature_platform_table_from_open_pr",
            ): "https://github.com/DayMarket/dbt-trino/pull/123",
        }
    finally:
        sync._run = original_run
        sync._open_pr_branch_tables = original_open_pr_branch_tables

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
      - name: feature_platform_disabled_table
        columns:
          - name: product_id
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
        ),
        (
            "silver",
            "feature_platform_disabled_table",
            None,
        )
    ]
    repaired_yaml, removed_tables = sync._remove_misplaced_tables_from_content(
        source_yaml,
        desired_schemas,
        {"feature_platform_disabled_table"},
    )
    assert (
        "feature_platform_disabled_table" in repaired_yaml
    )
    assert ("silver", "feature_platform_disabled_table", None) not in removed_tables
    assert sync._extract_source_tables(repaired_yaml) == {
        ("silver", "feature_platform_sku_group_orders"),
        ("silver", "feature_platform_disabled_table"),
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
            "create_dbt_pr": True,
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

        dataset_table_yaml = sync.render_source_yaml(
            dbt_config,
            dataset_table,
            "master",
            include_document_header=False,
            include_source_header=False,
        )
        sync._append_table_to_source_block(
            sources_path,
            "silver",
            dataset_table_yaml,
        )

        final_yaml = sources_path.read_text(encoding="utf-8")
        assert sync._extract_source_tables(final_yaml) == {
            ("silver", "feature_platform_sku_group_orders"),
            ("silver", "feature_platform_disabled_table"),
            ("silver", "external_removed_table"),
            ("silver", "feature_platform_sku_group_id_prices"),
            ("silver", "feature_platform_dataset_search_ranking_v1"),
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
