import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

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

# Три энтити, довешенные к feature_stats отдельно от аплоад-источников выше: две
# silver (search_query_sku_group_es_features, sku_group_id_prices) и одна gold вне
# пути ranking/dynamic-pricing аплоада (feedback_product_id). Отдельная константа,
# а не расширение UPLOAD_SOURCE_DAGS, — тот список специально про источники аплоада,
# эти три им не являются.
OTHER_WIRED_DAGS = (
    "layers/gold/product_id/feedback_product_id/v1/dag.py",
    "layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py",
    "layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py",
)

# Переменная партиции, которую dag.py передаёт и в build_dq_task, и в
# build_feature_stats_task. У большинства энтити это модульная константа
# DQ_PARTITION_DATE, но у search_query_sku_group_es_features — локальная переменная
# partition_date_arg (поддерживает ручной backfill через dag_run.conf), поэтому имя
# переменной задаётся явно, а не выводится по общему шаблону.
OTHER_WIRED_DAGS_PARTITION_ARG = {
    "layers/gold/product_id/feedback_product_id/v1/dag.py": "DQ_PARTITION_DATE",
    "layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py": "partition_date_arg",
    "layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py": "DQ_PARTITION_DATE",
}


def calls(dag_path: Path, name: str) -> bool:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name:
            return True
    return False


def test_every_upload_source_dag_builds_the_task() -> None:
    for relative in UPLOAD_SOURCE_DAGS + OTHER_WIRED_DAGS:
        dag_path = Path(relative)
        assert dag_path.is_file(), relative
        assert calls(dag_path, "build_feature_stats_task"), f"{relative}: нет build_feature_stats_task"


def test_task_runs_in_parallel_with_dq_and_has_no_downstream() -> None:
    for relative in UPLOAD_SOURCE_DAGS + OTHER_WIRED_DAGS:
        text = Path(relative).read_text(encoding="utf-8")
        assert ">> [dq_task, stats_task]" in text, f"{relative}: таска не параллельна dq"
        # Downstream на stats_task вешать нельзя: падение статистики не должно
        # блокировать публикацию фич, аплоад ждёт именно dq.
        assert "stats_task >>" not in text, f"{relative}: у stats_task не должно быть downstream"


def test_every_upload_source_dag_has_a_feature_stats_block() -> None:
    for relative in UPLOAD_SOURCE_DAGS + OTHER_WIRED_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert isinstance(config.get("feature_stats"), dict), f"{config_path}: нет блока feature_stats:"


def test_partition_settings_match_the_dq_block() -> None:
    keys = ("partition_column", "partition_granularity", "partition_date_template",
            "snapshot_interval_hours")
    for relative in UPLOAD_SOURCE_DAGS + OTHER_WIRED_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        for key in keys:
            assert config["dq"].get(key) == config["feature_stats"].get(key), (
                f"{config_path}: {key} расходится между dq: и feature_stats:"
            )


def test_other_wired_dags_pass_the_same_partition_value_to_both_tasks() -> None:
    for relative, arg_name in OTHER_WIRED_DAGS_PARTITION_ARG.items():
        text = Path(relative).read_text(encoding="utf-8")
        assert (
            f"build_feature_stats_task(CONFIG_PATH, REPO_ROOT)({arg_name})" in text
        ), f"{relative}: статистика должна получать ту же переменную партиции, что и dq ({arg_name})"


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


def test_every_dq_dag_also_builds_feature_stats() -> None:
    """
    Обходим все dag.py репозиторно-управляемых энтити (layers/ и datasets/) и
    проверяем правило AGENTS.md `## Feature Stats`: `feature_stats` — такой же
    обязательный шаг DAG'а энтити, как и `dq`. Значит, у любого DAG'а, который
    вызывает build_dq_task, обязан быть и вызов build_feature_stats_task —
    иначе новая энтити может получить dq и молча остаться без feature_stats,
    хотя список выше её не знает.

    Намеренно НЕ проверяем обратное (feature_stats есть, а dq нет) и не требуем
    dq вообще: у 40 энтити ещё нет таски dq, потому что миграция DQ в Feature
    Platform идёт поэтапно, и это легитимное промежуточное состояние, а не баг.
    "Ужесточение" этой проверки до «у каждого DAG'а обязателен dq» превратило бы
    её в постоянно красную сборку — не делайте так.
    """
    dag_paths = sorted((REPO_ROOT / "layers").rglob("dag.py")) + sorted(
        (REPO_ROOT / "datasets").rglob("dag.py")
    )
    assert dag_paths, "не нашли ни одного dag.py под layers/ или datasets/"

    for dag_path in dag_paths:
        if not calls(dag_path, "build_dq_task"):
            continue
        relative = dag_path.relative_to(REPO_ROOT)
        assert calls(dag_path, "build_feature_stats_task"), (
            f"{relative}: DAG вызывает build_dq_task, но не build_feature_stats_task. "
            "Нужно подключить feature_stats.task.build_feature_stats_task параллельно "
            "dq (<upstream> >> [dq_task, stats_task]) — см. AGENTS.md `## Feature Stats`."
        )


def main() -> int:
    test_every_upload_source_dag_builds_the_task()
    test_task_runs_in_parallel_with_dq_and_has_no_downstream()
    test_every_upload_source_dag_has_a_feature_stats_block()
    test_partition_settings_match_the_dq_block()
    test_other_wired_dags_pass_the_same_partition_value_to_both_tasks()
    test_snapshot_dag_passes_a_timestamp_template_to_both_tasks()
    test_daily_dags_pass_the_date_template_to_both_tasks()
    test_only_the_conversion_table_excludes_a_column()
    test_every_dq_dag_also_builds_feature_stats()
    print("Feature stats task wiring tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
