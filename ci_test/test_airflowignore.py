"""Проверка .airflowignore: тестовый код не должен попадать в парсер DAG'ов.

Airflow импортирует каждый .py под своей dags-директорией, если файл проходит
safe mode (содержит подстроки "airflow" и "dag"). Файлы из ci_test/ написаны под
pytest, а pytest в Airflow-образе не установлен, поэтому любой такой файл валит
парсер с ModuleNotFoundError: No module named 'pytest'. Единственная защита —
запись в .airflowignore.

Здесь воспроизведена логика Airflow для синтаксиса glob
(_GlobIgnoreRule в airflow/utils/file.py): паттерн со слэшем матчится против
пути относительно каталога с .airflowignore, паттерн без слэша — против имени.
"""

import fnmatch
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
IGNORE_FILE = REPO_ROOT / ".airflowignore"

# Строки, которые Airflow ищет в файле в safe mode перед импортом.
SAFE_MODE_MARKERS = ("dag", "airflow")


def load_rules():
    rules = []
    for line in IGNORE_FILE.read_text().splitlines():
        pattern = re.sub(r"\s*#.*", "", line).strip()
        if not pattern:
            continue
        relative_to = REPO_ROOT if "/" in pattern else None
        rules.append((re.compile(fnmatch.translate(pattern)), relative_to, pattern))
    return rules


def is_ignored(path: Path) -> bool:
    for regex, relative_to, _ in load_rules():
        rel = str(path.relative_to(relative_to)) if relative_to else path.name
        if regex.match(rel):
            return True
    return False


def might_contain_dag(path: Path) -> bool:
    """Airflow safe mode: импортируем файл, только если в нём есть оба маркера."""
    content = path.read_text(errors="ignore").lower()
    return all(marker in content for marker in SAFE_MODE_MARKERS)


def test_ci_test_directory_is_ignored():
    """Ни один файл ci_test/ не должен доходить до парсера DAG'ов."""
    # Файл из чужой ветки, уронивший парсер: его в дереве может не быть,
    # но правило обязано покрывать любое имя внутри ci_test/.
    probe = REPO_ROOT / "ci_test" / "test_demand_sku_sales_orchestration.py"
    assert is_ignored(probe), f"{probe.relative_to(REPO_ROOT)} не покрыт .airflowignore"

    not_ignored = [
        path.relative_to(REPO_ROOT)
        for path in sorted((REPO_ROOT / "ci_test").rglob("*.py"))
        if not is_ignored(path)
    ]
    assert not not_ignored, f"ci_test не покрыт .airflowignore: {not_ignored}"


def test_helper_directories_stay_ignored():
    """Существующие записи .airflowignore продолжают работать."""
    for relative in (
        "dq/runner.py",
        "feature_stats/runner.py",
        "layers/gold/sku_group_id/sku_group_stock_features/v1/job/job.py",
        "layers/gold/sku_group_id/sku_group_stock_features/v1/config/factory.py",
    ):
        path = REPO_ROOT / relative
        assert is_ignored(path), f"{relative} перестал игнорироваться"


def test_real_dags_are_not_ignored():
    """Защита от слишком широкого паттерна: DAG'и обязаны остаться видимыми."""
    dags = sorted(REPO_ROOT.glob("layers/*/*/*/*/dag.py"))
    assert dags, "не найдено ни одного layers/**/dag.py — проверка бессмысленна"
    ignored = [path.relative_to(REPO_ROOT) for path in dags if is_ignored(path)]
    assert not ignored, f".airflowignore скрыл настоящие DAG'и: {ignored}"


def test_no_pytest_import_reaches_the_parser():
    """Ни один видимый парсеру файл не тянет pytest на уровне модуля."""
    offenders = []
    for path in sorted(REPO_ROOT.rglob("*.py")):
        if ".git" in path.parts or "__pycache__" in path.parts:
            continue
        if is_ignored(path) or not might_contain_dag(path):
            continue
        if re.search(r"^\s*(import pytest|from pytest\b)", path.read_text(errors="ignore"), re.M):
            offenders.append(path.relative_to(REPO_ROOT))
    assert not offenders, f"парсер DAG'ов импортирует файлы с pytest: {offenders}"


def main() -> int:
    test_ci_test_directory_is_ignored()
    test_helper_directories_stay_ignored()
    test_real_dags_are_not_ignored()
    test_no_pytest_import_reaches_the_parser()
    print("airflowignore tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
