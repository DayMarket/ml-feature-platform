import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PILOT_DAGS = (
    "layers/silver/sku_group_id/sku_group_id_prices/v1/dag.py",
    "layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py",
    "layers/gold/product_id/feedback_product_id/v1/dag.py",
)


def uses_build_dq_task(dag_path: Path) -> bool:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "build_dq_task":
            return True
    return False


def test_pilot_dags_build_the_dq_task() -> None:
    for relative in PILOT_DAGS:
        dag_path = Path(relative)
        assert dag_path.is_file(), relative
        assert uses_build_dq_task(dag_path), f"{relative}: нет вызова build_dq_task"


def test_pilot_dags_declare_dq_as_terminal_task() -> None:
    for relative in PILOT_DAGS:
        text = Path(relative).read_text(encoding="utf-8")
        assert ">> dq_task" in text, f"{relative}: таска dq не подключена в конце графа"
        assert "dq_task >>" not in text, f"{relative}: таска dq не должна иметь downstream внутри DAG'а"


def main() -> int:
    test_pilot_dags_build_the_dq_task()
    test_pilot_dags_declare_dq_as_terminal_task()
    print("DQ task wiring tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
