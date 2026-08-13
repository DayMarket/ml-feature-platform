import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_generator_module():
    module_path = ROOT / "scripts" / "generate_feature_platform_map.py"
    spec = importlib.util.spec_from_file_location("generate_feature_platform_map", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_generated_feature_platform_map_is_current():
    generator = load_generator_module()
    expected = generator.render_repository_map(ROOT)
    actual = (ROOT / "docs" / "feature_platform_map.md").read_text(encoding="utf-8")
    assert actual == expected
    assert "## 1. Production-critical DAGs — Search" in actual
    assert "## 2. Production-critical DAGs — Logistics" in actual
    assert "## 3. Offline, training and backfill DAGs" in actual
    assert "title Production-critical DAG starts (UTC)" in actual
    assert "title Logistics-critical DAG starts (UTC)" in actual
    assert "title Offline, training and backfill DAG starts (UTC)" in actual
    assert "section Production-critical" in actual
    assert "section Logistics" in actual
    assert "section Training" in actual
    assert "section Backfill" in actual
    assert "section Other" in actual
    assert "UTC 03:00" in actual


def test_map_contains_cross_dag_dependencies_and_schedules():
    generator = load_generator_module()
    records = {record.dag_id: record for record in generator.discover_dags(ROOT)}

    qid_id = (
        "feature-platform.layers.gold.query_sku_group_id."
        "sku_group_query_atc_order_features_qid"
    )
    qid = records[qid_id]
    assert qid.schedule == "0 6 * * *"
    assert generator.Dependency(
        "feature-platform.layers.gold.query_text_version.search_query_id",
        "sensor",
        60,
    ) in qid.dependencies

    query_id = records[
        "feature-platform.layers.gold.query_text_version.search_query_id"
    ]
    assert generator.Dependency(
        "dbt.source.trino.ml_feature_platform_silver."
        "feature_platform_search_sku_group_id_install_query.dq",
        "sensor",
        240,
    ) in query_id.dependencies

    es_writer_id = (
        "feature-platform.layers.silver.query_sku_group_id."
        "search_query_sku_group_es_features"
    )
    es_collect_id = f"{es_writer_id}.elasticsearch_collect"
    assert generator.Dependency(es_collect_id, "sensor", 0) in records[es_writer_id].dependencies

    ranking_upload = records["feature_platform_ranking_features_upload_dag"]
    assert generator.Dependency(
        "feature-platform.layers.gold.query_sku_group_id."
        "sku_group_query_atc_order_features.v2",
        "upload-sensor",
        60,
    ) in ranking_upload.dependencies

    dataset = records["feature-platform.datasets.search.search_ranking.v1"]
    assert dataset.severity == "P4"
    assert dataset.workload == "training"
    assert dataset.expected_severity == "P4"

    logistics_records = [
        record
        for record in records.values()
        if record.group_tag in generator._critical_group_tags(ROOT, "Logistics")
        or record.dag_id in generator._critical_dag_ids(ROOT, "Logistics")
    ]
    assert len(logistics_records) == 8
    assert {record.area for record in logistics_records} == {"silver", "gold"}
    assert (
        "feature-platform.layers.silver.category_level_category_id."
        "order_completion_category_features"
    ) not in {record.dag_id for record in logistics_records}

    assert generator.severity_policy_violations(list(records.values())) == []

    production_records = [
        record for record in records.values() if record.workload == "production"
    ]
    offline_records = [
        record for record in records.values() if record.workload != "production"
    ]
    assert ranking_upload in production_records
    assert dataset in offline_records


def test_paused_dags_are_excluded_from_map():
    generator = load_generator_module()
    records = {record.dag_id: record for record in generator.discover_dags(ROOT)}
    excluded_dags = generator._map_exclusions(ROOT)
    rendered = generator.render_repository_map(ROOT)

    assert excluded_dags
    for paused_dag_id in excluded_dags:
        assert paused_dag_id not in records
        assert f"`{paused_dag_id}`" not in rendered


if __name__ == "__main__":
    test_generated_feature_platform_map_is_current()
    test_map_contains_cross_dag_dependencies_and_schedules()
    test_paused_dags_are_excluded_from_map()
    print("Feature Platform map tests completed successfully")
