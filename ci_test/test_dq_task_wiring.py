import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PILOT_DAGS = (
    "layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py",
    "layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py",
    "layers/gold/product_id/feedback_product_id/v1/dag.py",
)

# Все gold-источники ranking- и dynamic-pricing-аплоада: аплоад ждёт именно их таску dq.
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

DQ_OWNING_DAGS = PILOT_DAGS + UPLOAD_SOURCE_DAGS


def uses_build_dq_task(dag_path: Path) -> bool:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "build_dq_task":
            return True
    return False


def test_pilot_dags_build_the_dq_task() -> None:
    for relative in DQ_OWNING_DAGS:
        dag_path = Path(relative)
        assert dag_path.is_file(), relative
        assert uses_build_dq_task(dag_path), f"{relative}: нет вызова build_dq_task"


def test_pilot_dags_declare_dq_as_terminal_task() -> None:
    for relative in DQ_OWNING_DAGS:
        text = Path(relative).read_text(encoding="utf-8")
        # У UPLOAD_SOURCE_DAGS с таской feature_stats dq_task подключается вместе
        # со stats_task одной параллельной связкой ">> [dq_task, stats_task]" —
        # это тот же факт "dq_task подключена и терминальна", просто в списочной
        # форме (см. ci_test/test_feature_stats_task_wiring.py).
        wired = ">> dq_task" in text or ">> [dq_task, stats_task]" in text
        assert wired, f"{relative}: таска dq не подключена в конце графа"
        assert "dq_task >>" not in text, f"{relative}: таска dq не должна иметь downstream внутри DAG'а"


def test_every_dq_owning_dag_has_a_dq_config_block() -> None:
    """Таска dq без блока dq: молча поедет на дефолтных порогах — это не контракт."""
    import yaml

    for relative in UPLOAD_SOURCE_DAGS:
        config_path = Path(relative).parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert isinstance(config.get("dq"), dict), f"{config_path}: нет блока dq:"
        assert config["dq"].get("tests"), f"{config_path}: dq.tests пуст"


def test_snapshot_dag_passes_a_timestamp_template() -> None:
    """Снапшотной энтити нужен data_interval_end со временем, иначе DQ упадёт на разборе."""
    relative = (
        "layers/gold/calculated_at_sku_group_id_promotion_id/"
        "dynamic_pricing_sku_group_price_features/v1/dag.py"
    )
    text = Path(relative).read_text(encoding="utf-8")
    assert 'data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S")' in text


def main() -> int:
    test_pilot_dags_build_the_dq_task()
    test_pilot_dags_declare_dq_as_terminal_task()
    test_every_dq_owning_dag_has_a_dq_config_block()
    test_snapshot_dag_passes_a_timestamp_template()
    print("DQ task wiring tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
