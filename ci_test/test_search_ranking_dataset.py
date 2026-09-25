import importlib.util
import re
import sys
import types
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
ENTITY_DIR = ROOT / "datasets" / "search" / "search_ranking" / "v1"
CONFIG_PATH = ENTITY_DIR / "config.yaml"
MIGRATIONS_DIR = ENTITY_DIR / "migrations"
DDL_PATH = MIGRATIONS_DIR / "create_table.sql"

COLUMN_DEFINITION = re.compile(
    r"^\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+[A-Za-z]", re.MULTILINE
)
ADDED_COLUMN = re.compile(
    r"ADD COLUMN IF NOT EXISTS\s+[`\"]?([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE
)

# Цены показа. sell_price и seller_price — разные поля одного события: на замере
# от 2026-09-15 (85 980 264 показа после фильтров джоба) они расходятся на 8.4%
# показов, поэтому выпадение любого из них обязано ронять тест, а не проходить
# незамеченным из-за похожих имён.
IMPRESSION_PRICE_COLUMNS = ("sell_price", "seller_price", "final_price")


def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def load_job_module():
    # Заглушки pyspark и пакет job ставятся только на время импорта: оставленные
    # в sys.modules, они подменили бы заглушки соседних тестов, которые грузят
    # свой собственный пакет job (test_query_id_features, test_query_category_*).
    job_name = "job.getting_search_ranking_dataset"
    touched = ("pyspark", "pyspark.sql", "job", "job.entities", job_name)
    saved = {name: sys.modules.get(name) for name in touched}
    try:
        pyspark_module = types.ModuleType("pyspark")
        pyspark_sql_module = types.ModuleType("pyspark.sql")
        pyspark_sql_module.SparkSession = object
        pyspark_module.sql = pyspark_sql_module
        sys.modules["pyspark"] = pyspark_module
        sys.modules["pyspark.sql"] = pyspark_sql_module
        job_package = types.ModuleType("job")
        job_package.__path__ = [str(ENTITY_DIR / "job")]
        sys.modules["job"] = job_package
        for name in ("entities", "getting_search_ranking_dataset"):
            spec = importlib.util.spec_from_file_location(
                f"job.{name}", ENTITY_DIR / "job" / f"{name}.py"
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[f"job.{name}"] = module
            spec.loader.exec_module(module)
        return sys.modules[job_name]
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


class _RecordingSpark:
    """Перехватывает единственный spark.sql() джоба, чтобы получить текст запроса."""

    def __init__(self):
        self.sql_text = None

    def sql(self, query: str):
        self.sql_text = query
        return query


def build_sql() -> str:
    spark = _RecordingSpark()
    load_job_module().build_search_ranking_dataset(spark, "2026-09-15T00:00:00+00:00")
    assert spark.sql_text is not None, "джоб не вызвал spark.sql"
    return spark.sql_text


def ddl_columns() -> list[str]:
    body = DDL_PATH.read_text(encoding="utf-8")
    body = body[body.index("(") + 1 : body.index("\n)\nUSING iceberg")]
    return COLUMN_DEFINITION.findall(body)


def final_select_aliases(sql: str) -> list[str]:
    projection = sql[sql.rindex("\n)\nSELECT\n") + len("\n)\nSELECT\n") :]
    projection = projection[: projection.index("\nFROM sessions s")]

    aliases = []
    for line in projection.splitlines():
        item = line.strip().rstrip(",")
        if not item or item.startswith("--"):
            continue
        item = re.split(r"\s+AS\s+", item)[-1]
        aliases.append(item.rsplit(".", 1)[-1].strip("`"))
    return aliases


class SearchRankingDatasetTest(unittest.TestCase):
    def test_ddl_columns_match_the_job_projection(self):
        # Порядок — контракт между DDL и финальным SELECT'ом. DataFrameWriterV2.
        # overwritePartitions() резолвит колонки по имени, так что съехавший
        # порядок сам по себе не ломает запись; пиннится он, чтобы схема и
        # проекция читались бок о бок и не расходились по составу.
        self.assertEqual(final_select_aliases(build_sql()), ddl_columns())

    def test_impression_price_columns_are_collected(self):
        columns = ddl_columns()
        for column in IMPRESSION_PRICE_COLUMNS:
            with self.subTest(column=column):
                self.assertIn(column, columns)

    def test_sell_price_is_read_from_the_flat_events_column(self):
        # events.sell_price совпал с event_properties.event_parameters.sell_price
        # на всех 85 980 262 непустых показах замера от 2026-09-15, включая
        # совпадение NULL, поэтому JSON здесь не парсится — как и у bid_id.
        # seller_price, наоборот, плоской колонкой подменять нельзя: там
        # расхождение реальное, и этот тест не должен подтолкнуть к обратному.
        sql = build_sql()

        self.assertIn("CAST(sell_price AS BIGINT) AS sell_price", sql)
        # Именно вызов, а не подстрока: путь до поля разрешено упоминать в
        # комментарии, запрещено читать его из JSON.
        self.assertNotIn(
            "get_json_object(event_properties, '$.event_parameters.sell_price')", sql
        )
        self.assertIn("get_json_object(event_properties, '$.event_parameters.seller_price')", sql)

    def test_migrations_add_only_columns_declared_in_create_table(self):
        # Новое окружение получает схему из create_table.sql, существующее — из
        # миграций. Колонка, добавленная только миграцией, разъехала бы их.
        declared = set(ddl_columns())
        for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if migration.name == "create_table.sql":
                continue
            for column in ADDED_COLUMN.findall(migration.read_text(encoding="utf-8")):
                with self.subTest(migration=migration.name, column=column):
                    self.assertIn(column, declared)

    def test_sell_price_is_covered_by_an_idempotent_migration(self):
        # Таблица уже существует в проде, поэтому одной правки create_table.sql
        # мало: без ALTER TABLE колонка не появится в существующем окружении.
        added = set()
        for migration in MIGRATIONS_DIR.glob("*.sql"):
            if migration.name == "create_table.sql":
                continue
            added.update(ADDED_COLUMN.findall(migration.read_text(encoding="utf-8")))

        self.assertIn("sell_price", added)

    def test_sell_price_has_a_not_null_dq_test(self):
        # Покрытие 99.999998% (2 пустых на 85 980 264 показа замера), поэтому NULL
        # здесь означает сломавшийся контракт источника, а не редкий валидный
        # случай. severity warn — как у всех тестов этой энтити.
        specs = [
            spec
            for spec in load_config()["dq"]["tests"]
            if spec["name"] == "not_null" and "sell_price" in spec.get("columns", [])
        ]

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0]["severity"], "warn")

    def test_sell_price_stays_in_the_feature_stats_profile(self):
        # Цена — признак, а не идентификатор: в exclude_columns ей не место,
        # иначе распределение цен показа перестанет наблюдаться.
        self.assertNotIn(
            "sell_price", load_config()["feature_stats"].get("exclude_columns", [])
        )


if __name__ == "__main__":
    unittest.main()
