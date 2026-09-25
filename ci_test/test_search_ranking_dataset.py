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


def strip_sql_comments(sql: str) -> str:
    """Убирает строки-комментарии: снятый фильтр разрешено объяснять словами."""
    return "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )


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

    def test_has_search_attr_is_declared_next_to_is_generated_order(self):
        # Новая атрибуция — отдельная метка рядом со старым таргетом, а не его
        # замена: is_generated_order обязан остаться в витрине нетронутым.
        columns = ddl_columns()

        self.assertIn("is_generated_order", columns)
        self.assertIn("has_search_attr", columns)

    def test_has_search_attr_is_aggregated_on_the_is_generated_order_grain(self):
        # Метка собирается своим агрегатом, но на той же грани, что и
        # is_generated_order (install_id, last_search_session_id, query,
        # sku_group_id), и тем же правилом "хотя бы один заказ" -> константа 1
        # плюс COALESCE(..., 0) для показов без заказа. Иначе два таргета одной
        # строки означали бы разные вещи.
        sql = build_sql()
        aggregate = sql[
            sql.index("search_attr_orders AS (") : sql.index("sessions_raw AS (")
        ]

        self.assertIn("1 AS has_search_attr", aggregate)
        self.assertIn("FROM search_attributed_orders sao", aggregate)
        self.assertIn("INNER JOIN order_items_enhanced oie", aggregate)
        for key in (
            "sao.last_search_session_id",
            "sao.install_id",
            "sao.query",
            "oie.sku_group_id",
        ):
            with self.subTest(key=key):
                self.assertIn(key, aggregate.split("GROUP BY")[1])

        self.assertIn("COALESCE(sa.has_search_attr, 0) AS has_search_attr", sql)

    def test_attribution_is_joined_twice_on_the_same_keys(self):
        # Два независимых LEFT JOIN'а к атрибуции: старый таргет и новая метка
        # собираются из разных наборов строк, поэтому одним джоином их не
        # получить. Ключ у обоих один, иначе метки поехали бы относительно
        # друг друга. Каждый агрегат даёт одну строку на ключ (GROUP BY), так
        # что второй джоин не размножает показы.
        sql = build_sql()
        tail = sql[sql.index("\nFROM sessions s") :]

        self.assertIn("LEFT JOIN orders o", tail)
        self.assertIn("LEFT JOIN search_attr_orders sa", tail)
        for alias in ("o", "sa"):
            with self.subTest(alias=alias):
                self.assertIn(f"ON {alias}.install_id = s.install_id", tail)
                self.assertIn(f"AND {alias}.last_search_session_id = s.session_id", tail)
                self.assertIn(f"AND {alias}.sku_group_id = s.sku_group_id", tail)
                self.assertIn(f"AND {alias}.query = s.query", tail)

    def test_has_search_attr_is_read_from_the_attribution_source(self):
        # Флаг живёт в iceberg.silver.order_items_attribution и задаёт набор
        # строк своей CTE; брать его из order_items или events нельзя - это
        # поле атрибуции, а не заказа и не показа.
        sql = strip_sql_comments(build_sql())
        search_attributed = sql[
            sql.index("search_attributed_orders AS (") : sql.index(
                "order_items_enhanced AS ("
            )
        ]

        self.assertIn("FROM iceberg.silver.order_items_attribution", search_attributed)
        self.assertIn("\n        has_search_attr\n", search_attributed)

    def test_search_attribution_branch_filters_on_the_flag_only(self):
        # Контракт метки: важен сам флаг, а не то, откуда пришел заказ. Поэтому
        # вторая CTE не наследует ни один бизнес-фильтр ветки
        # is_generated_order - ни список widget_space_name, ни is_full_catpred,
        # ни query != ''. Из фильтров остается только окно партиции.
        # Возврат любого из них - это смена контракта метки, а не рефакторинг:
        # на 2026-09-15 фильтры срезают ключи с 138 665 до 40 230.
        # Комментарии выброшены: снятые фильтры разрешено объяснять словами,
        # запрещено применять.
        sql = strip_sql_comments(build_sql())
        attributed = sql[
            sql.index("attributed_orders AS (") : sql.index(
                "search_attributed_orders AS ("
            )
        ]
        search_attributed = sql[
            sql.index("search_attributed_orders AS (") : sql.index(
                "order_items_enhanced AS ("
            )
        ]

        self.assertIn("widget_space_name IN (", attributed)
        self.assertIn("COALESCE(is_full_catpred, 'false') = 'false'", attributed)
        self.assertIn("query != ''", attributed)
        self.assertNotIn("has_search_attr", attributed)

        self.assertIn("has_search_attr", search_attributed)
        self.assertNotIn("widget_space_name", search_attributed)
        self.assertNotIn("is_full_catpred", search_attributed)
        self.assertNotIn("query != ''", search_attributed)

    def test_order_item_status_is_filtered_inside_the_is_generated_order_branch(self):
        # order_item_status - фильтр старого таргета, а не общей CTE: оставь он
        # в order_items_enhanced, новая метка молча унаследовала бы его. Обе
        # ветки по-прежнему читают order_items одним сканом, ограниченным
        # окном generated_at.
        sql = strip_sql_comments(build_sql())
        items = sql[
            sql.index("order_items_enhanced AS (") : sql.index("\norders AS (")
        ]
        orders = sql[sql.index("\norders AS (") : sql.index("search_attr_orders AS (")]
        search_attr_orders = sql[
            sql.index("search_attr_orders AS (") : sql.index("sessions_raw AS (")
        ]

        self.assertNotIn("order_item_status NOT IN", items)
        self.assertIn("oi.generated_at >=", items)
        self.assertIn("oie.order_item_status NOT IN ('CREATED', 'NOT_CREATED')", orders)
        self.assertNotIn("order_item_status", search_attr_orders)

    def test_has_search_attr_is_covered_by_an_idempotent_migration(self):
        # Таблица уже существует в проде: без ALTER TABLE колонка не появится
        # в существующем окружении, только в новом из create_table.sql.
        added = set()
        for migration in MIGRATIONS_DIR.glob("*.sql"):
            if migration.name == "create_table.sql":
                continue
            added.update(ADDED_COLUMN.findall(migration.read_text(encoding="utf-8")))

        self.assertIn("has_search_attr", added)

    def test_has_search_attr_has_a_not_null_dq_test(self):
        # После COALESCE колонка заполнена всегда, а в источнике за
        # event_received_at = 2026-09-15 (284 605 строк) NULL не встретился ни
        # разу, поэтому NULL здесь означает сломавшийся контракт источника.
        # severity warn — как у всех тестов этой энтити.
        specs = [
            spec
            for spec in load_config()["dq"]["tests"]
            if spec["name"] == "not_null" and "has_search_attr" in spec.get("columns", [])
        ]

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0]["severity"], "warn")

    def test_has_search_attr_stays_in_the_feature_stats_profile(self):
        # Метка, а не идентификатор: как и у is_generated_order, доля единиц в
        # партиции — то, по чему видно, что атрибуция поехала.
        exclude = load_config()["feature_stats"].get("exclude_columns", [])

        self.assertNotIn("has_search_attr", exclude)
        self.assertNotIn("is_generated_order", exclude)

    def test_sell_price_stays_in_the_feature_stats_profile(self):
        # Цена — признак, а не идентификатор: в exclude_columns ей не место,
        # иначе распределение цен показа перестанет наблюдаться.
        self.assertNotIn(
            "sell_price", load_config()["feature_stats"].get("exclude_columns", [])
        )


if __name__ == "__main__":
    unittest.main()
