import importlib.util
import tempfile
from pathlib import Path


def load_sync_module():
    module_path = Path("scripts/sync_iceberg_maintenance.py")
    spec = importlib.util.spec_from_file_location("sync_iceberg_maintenance", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    sync = load_sync_module()

    discovered_tables = sync.discover_iceberg_tables(Path("."))
    assert "gold" in discovered_tables
    assert "silver" in discovered_tables
    assert "feature_platform_sku_group_price_features" in discovered_tables["gold"]
    assert "feature_platform_sku_group_id_prices" in discovered_tables["silver"]
    assert "feature_platform_dataset_search_ranking_v1" in discovered_tables["silver"]

    existing_config = """# Manual file
schemas:
  gold:
    - manually_added_gold_table
    - feature_platform_sku_group_price_features
  silver:
    - manually_added_silver_table
"""
    parsed = sync.parse_schema_table_config(existing_config)
    diff_config = """diff --git a/dags_v3/maintenance_generator/feature_platform_config.yaml b/dags_v3/maintenance_generator/feature_platform_config.yaml
@@ -1,4 +1,6 @@
+schemas:
+  gold:
+    - feature_platform_new_gold_table
"""
    assert sync.parse_schema_table_keys(diff_config, strip_diff=True) == {
        ("gold", "feature_platform_new_gold_table")
    }

    missing = sync.missing_schema_tables(
        {
            "gold": [
                "feature_platform_sku_group_price_features",
                "feature_platform_pending_table",
                "feature_platform_new_table",
            ],
        },
        parsed,
        {
            (
                "gold",
                "feature_platform_pending_table",
            ): "https://github.com/DayMarket/pyspark-etl/pull/1",
        },
    )
    assert missing == {
        "gold": ["feature_platform_new_table"],
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        assert sync.sync_maintenance_files(
            repo,
            {
                "gold": ["feature_platform_table_from_open_pr"],
            },
            {
                (
                    "gold",
                    "feature_platform_table_from_open_pr",
                ): "https://github.com/DayMarket/pyspark-etl/pull/2",
            },
        ) == []
        assert not (repo / sync.MAINTENANCE_CONFIG_PATH).exists()

    merged = sync.merge_schema_tables(
        parsed,
        {
            "gold": ["feature_platform_sku_group_price_features"],
            "silver": [
                "feature_platform_dataset_search_ranking_v1",
                "feature_platform_sku_group_id_prices",
            ],
        },
    )
    rendered = sync.render_feature_platform_config(merged)
    assert rendered.count("feature_platform_sku_group_price_features") == 1
    assert "manually_added_gold_table" in rendered
    assert "manually_added_silver_table" in rendered
    assert "feature_platform_dataset_search_ranking_v1" in rendered
    assert "feature_platform_sku_group_id_prices" in rendered

    dag_content = '''
def dpa_streamer_config() -> dict:
    return {"schemas": {}}

_CONFIG_LOADERS = {
    "local": local_config,
    "dpa": dpa_streamer_config,
}

create_dag(config_name="local",   dag_suffix="_dwh")
create_dag(config_name="dpa",     dag_suffix="_dpa")
'''
    updated_dag = sync.ensure_feature_platform_dag(dag_content)
    assert 'def feature_platform_config()' in updated_dag
    assert '"feature_platform": feature_platform_config,' in updated_dag
    assert 'create_dag(config_name="feature_platform", dag_suffix="_fp")' in updated_dag
    assert sync.ensure_feature_platform_dag(updated_dag) == updated_dag

    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        dag_path = repo / sync.MAINTENANCE_DAG_PATH
        dag_path.parent.mkdir(parents=True)
        dag_path.write_text(dag_content, encoding="utf-8")
        changed_files = sync.sync_maintenance_files(
            repo,
            {
                "gold": ["feature_platform_sku_group_price_features"],
                "silver": ["feature_platform_sku_group_id_prices"],
            },
        )
        assert {
            path.relative_to(repo).as_posix()
            for path in changed_files
        } == {
            sync.MAINTENANCE_CONFIG_PATH.as_posix(),
            sync.MAINTENANCE_DAG_PATH.as_posix(),
        }
        assert sync.sync_maintenance_files(
            repo,
            {
                "gold": ["feature_platform_sku_group_price_features"],
                "silver": ["feature_platform_sku_group_id_prices"],
            },
        ) == []

        assert sync.sync_maintenance_files(
            repo,
            {
                "gold": [
                    "feature_platform_sku_group_price_features",
                    "feature_platform_table_from_open_pr",
                ],
                "silver": ["feature_platform_sku_group_id_prices"],
            },
            {
                (
                    "gold",
                    "feature_platform_table_from_open_pr",
                ): "https://github.com/DayMarket/pyspark-etl/pull/2",
            },
        ) == []

    print("Iceberg maintenance sync tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
