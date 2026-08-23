import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

# Все gold-источники ranking- и dynamic-pricing-аплоада.
UPLOAD_SOURCE_DAGS = (
    "layers/gold/query/search_query_atc_features/v1/dag.py",
    "layers/gold/query_sku_group_id/sku_group_query_atc_order_features/v2/dag.py",
    "layers/gold/sku_group_id/sku_group_search_conversion_features/v2/dag.py",
    "layers/gold/sku_group_id/sku_group_stock_features/v1/dag.py",
    "layers/gold/sku_group_id/sku_group_price_features/v1/dag.py",
    "layers/gold/sku_group_id/feedback_sku_group_id/v1/dag.py",
    (
        "layers/gold/calculated_at_sku_group_id_promotion_id/"
        "dynamic_pricing_sku_group_price_features/v1/dag.py"
    ),
)

SNAPSHOT_DAG = UPLOAD_SOURCE_DAGS[-1]


def calls(dag_path: Path, name: str) -> bool:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name:
            return True
    return False


def test_every_upload_source_dag_builds_the_task() -> None:
    for relative in UPLOAD_SOURCE_DAGS:
        dag_path = Path(relative)
        assert dag_path.is_file(), relative
        assert calls(dag_path, "build_feature_stats_task"), f"{relative}: нет build_feature_stats_task"


def test_task_runs_in_parallel_with_dq_and_has_no_downstream() -> None:
    for relative in UPLOAD_SOURCE_DAGS:
        text = Path(relative).read_text(encoding="utf-8")
        assert ">> [dq_task, stats_task]" in text, f"{relative}: таска не параллельна dq"
        # Downstream на stats_task вешать нельзя: падение статистики не должно
        # блокировать публикацию фич, аплоад ждёт именно dq.
        assert "stats_task >>" not in text, f"{relative}: у stats_task не должно быть downstream"


def test_every_upload_source_dag_has_a_feature_stats_block() -> None:
    for relative in UPLOAD_SOURCE_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert isinstance(config.get("feature_stats"), dict), f"{config_path}: нет блока feature_stats:"


def test_partition_settings_match_the_dq_block() -> None:
    keys = ("partition_column", "partition_granularity", "partition_date_template",
            "snapshot_interval_hours")
    for relative in UPLOAD_SOURCE_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        for key in keys:
            assert config["dq"].get(key) == config["feature_stats"].get(key), (
                f"{config_path}: {key} расходится между dq: и feature_stats:"
            )


def test_snapshot_dag_passes_a_timestamp_template_to_both_tasks() -> None:
    """У снапшотной энтити константа называется DQ_PARTITION_TIMESTAMP, а не ..._DATE."""
    text = Path(SNAPSHOT_DAG).read_text(encoding="utf-8")
    template = 'data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S")'
    assert template in text
    assert (
        "build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_TIMESTAMP)" in text
    ), "статистике снапшотной энтити нужен тот же timestamp-шаблон, что и dq"


def test_daily_dags_pass_the_date_template_to_both_tasks() -> None:
    for relative in UPLOAD_SOURCE_DAGS[:-1]:
        text = Path(relative).read_text(encoding="utf-8")
        assert (
            "build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)" in text
        ), f"{relative}: статистика должна получать ту же константу партиции, что и dq"


def test_only_the_conversion_table_excludes_a_column() -> None:
    """category_id — единственная числовая не-фича среди семи таблиц."""
    for relative in UPLOAD_SOURCE_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        excluded = config["feature_stats"].get("exclude_columns") or []
        if "sku_group_search_conversion_features" in relative:
            assert excluded == ["category_id"], config_path
        else:
            assert excluded == [], config_path


def main() -> int:
    test_every_upload_source_dag_builds_the_task()
    test_task_runs_in_parallel_with_dq_and_has_no_downstream()
    test_every_upload_source_dag_has_a_feature_stats_block()
    test_partition_settings_match_the_dq_block()
    test_snapshot_dag_passes_a_timestamp_template_to_both_tasks()
    test_daily_dags_pass_the_date_template_to_both_tasks()
    test_only_the_conversion_table_excludes_a_column()
    print("Feature stats task wiring tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
