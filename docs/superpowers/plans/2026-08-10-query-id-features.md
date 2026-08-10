# query_id-Densified Search Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Посчитать существующие поисковые фичи, агрегируя события в рамках каноничного `query_id`, и отдать результат по-прежнему на исходных `query_text`, чтобы ключ джойна в ranking-сервисе не менялся.

**Architecture:** Две новые gold-сущности — покомпонентные зеркала `search_query_atc_features` (грейн `date,query`) и `sku_group_query_atc_order_features/v2` (грейн `date,query,sku_group_id`). В каждой оконные суммы группируются по `group_key = coalesce(query_id, query)` вместо `query`, после чего результат разворачивается обратно на все `query_text` группы. Старые таблицы не трогаются.

**Границы итерации:** `upload/features_service_upload/v1/**` в этой итерации **не изменяется**. Обе таблицы считаются и лежат в Iceberg, но в ranking-сервис не публикуются, поэтому аплоад-DAG остаётся в 04:00 и конфликта расписаний не возникает. Всё, что понадобится для публикации, описано в разделе «Отложено: публикация в ranking upload» в конце плана.

**Tech Stack:** PySpark 3.5.5 на shared-образе `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`, Iceberg 1.5.2, Airflow 3 (`SparkKubernetesOperator`, `CronDataIntervalTimetable`, `ExternalTaskSensor`), доставка кода через `git-sync`. Тесты — `unittest` из stdlib, запускаются как обычные python-скрипты.

**Спека:** `docs/superpowers/specs/2026-08-10-query-id-features-design.md`

## Global Constraints

- Все новые Iceberg-таблицы: `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')` в `CREATE TABLE IF NOT EXISTS`, комментарий на каждой колонке, партиционирование по `date`.
- `table.catalog: iceberg` во всех `config.yaml`. Обязательные поля: `table.catalog`, `table.schema`, `table.name`, `table.primary_key`, `table.meta.team`.
- `config.yaml` читается простым построчным парсером: только вложенные mapping-и, без YAML-якорей, списков и кавычек вокруг скаляров.
- Владение и алерты для обеих новых сущностей: `table.meta.team: team:search`, `dag.team: search`, `alerts.team: search`, `alerts.severity: P3`, `alerts.oncall_webhook_conn_id: oncall_webhook_search`.
- Нормализация запроса ровно в этом порядке: `lower` → `ё`→`е` → `\s+`→` ` → `trim`. Применяется и к событиям, и к `query_text` из справочника.
- Разбор границы интервала — только через тестируемый `parse_partition_date`. `partition_start[:10]` запрещён.
- Фичевые колонки новых таблиц — строгое зеркало оригиналов: те же имена, те же формулы, тот же порядок. Новых фичей не добавляем.
- Служебные колонки `query_id` и `has_query_id` присутствуют в таблицах, но фичами не считаются: в вектор ranking upload они не попадут, когда публикацию включат.
- `upload/features_service_upload/v1/**` в этой итерации не редактируется ни одной задачей.
- Отсечки топ-N нет: пишутся все пары.
- Никаких `DROP`/`DELETE`/`TRUNCATE` в миграциях.
- Новые DAG создаются с `is_paused_upon_creation=True`.

**Справочник и константы, используемые обеими сущностями:**

```python
QUERY_ID_TABLE = "iceberg.gold.feature_platform_search_query_id"
QUERY_ID_VERSION = "v1"
NORMALIZATION_REPLACEMENTS = (("ё", "е"), (r"\s+", " "))
```

---

### Task 1: Таблица уровня запроса — миграция и config

**Files:**
- Create: `layers/gold/query/search_query_atc_features_qid/v1/migrations/create_table.sql`
- Create: `layers/gold/query/search_query_atc_features_qid/v1/migrations/__init__.py` (пустой)
- Create: `layers/gold/query/search_query_atc_features_qid/v1/config.yaml`
- Create: `layers/gold/query/search_query_atc_features_qid/v1/config/factory.py`
- Test: `ci_test/test_query_id_features.py`

**Interfaces:**
- Consumes: ничего.
- Produces: таблица `iceberg.gold.feature_platform_search_query_atc_features_qid` с колонками `date, query, query_id, has_query_id` + 24 фичевые колонки, идентичные `feature_platform_search_query_atc_features`. Константа пути `layers/gold/query/search_query_atc_features_qid/v1` используется в задачах 2, 3 и 7.

- [ ] **Step 1: Создать директории и скопировать миграцию-основу**

```bash
mkdir -p layers/gold/query/search_query_atc_features_qid/v1/migrations
mkdir -p layers/gold/query/search_query_atc_features_qid/v1/config
mkdir -p layers/gold/query/search_query_atc_features_qid/v1/job
mkdir -p layers/gold/query/search_query_atc_features_qid/v1/entrypoints
touch layers/gold/query/search_query_atc_features_qid/v1/migrations/__init__.py
cp layers/gold/query/search_query_atc_features/v1/migrations/create_table.sql \
   layers/gold/query/search_query_atc_features_qid/v1/migrations/create_table.sql
cp layers/gold/query/search_query_atc_features/v1/config/factory.py \
   layers/gold/query/search_query_atc_features_qid/v1/config/factory.py
```

`config/factory.py` копируется без изменений: он читает соседний `config.yaml` и не содержит имён конкретной сущности.

- [ ] **Step 2: Добавить служебные колонки в миграцию**

В `layers/gold/query/search_query_atc_features_qid/v1/migrations/create_table.sql` заменить строку

```sql
    query STRING COMMENT 'Нормализованный текст поискового запроса',
```

на

```sql
    query STRING COMMENT 'Нормализованный текст поискового запроса',
    query_id STRING COMMENT 'Ключ группы, по которой посчитаны значения строки: каноничный query_id из feature_platform_search_query_id либо сам нормализованный query при отсутствии в справочнике',
    has_query_id BOOLEAN COMMENT 'true, если query нашёлся в справочнике feature_platform_search_query_id; false для фолбэка на сам запрос',
```

и заменить строку

```sql
COMMENT 'Gold-фичи количества поисковых показов, ATC и заказов по query'
```

на

```sql
COMMENT 'Gold-фичи количества поисковых показов, ATC и заказов по query, агрегированные в рамках query_id'
```

Остальные 24 фичевые колонки и их комментарии не трогать — это и есть строгое зеркало.

- [ ] **Step 3: Написать config.yaml**

```yaml
resources:
  path: ../../../../../config/spark/resources.yaml

spark:
  template_path: ../../../../../config/spark/layer_spark_application.yaml
  application_name: fetch-gold-search-query-atc-features-qid
  main_application_file: local:///git/repo/layers/gold/query/search_query_atc_features_qid/v1/entrypoints/get_search_query_atc_features_qid.py
  resource_profile: small

table:
  key: search_query_atc_features_qid
  catalog: iceberg
  schema: gold
  name: feature_platform_search_query_atc_features_qid
  primary_key: date,query
  meta:
    team: team:search
dag:
  team: search

alerts:
  team: search
  severity: P3
  oncall_webhook_conn_id: oncall_webhook_search
```

- [ ] **Step 4: Написать первый тест — зеркальность колонок**

Создать `ci_test/test_query_id_features.py`:

```python
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

QUERY_ORIGIN = ROOT / "layers" / "gold" / "query" / "search_query_atc_features" / "v1"
QUERY_QID = ROOT / "layers" / "gold" / "query" / "search_query_atc_features_qid" / "v1"

SERVICE_COLUMNS = ("query_id", "has_query_id")

COLUMN_PATTERN = re.compile(r"^\s{4}([A-Za-z_][A-Za-z0-9_]*)\s+[A-Z]", re.MULTILINE)


def migration_columns(entity_dir: Path) -> list[str]:
    sql = (entity_dir / "migrations" / "create_table.sql").read_text(encoding="utf-8")
    return COLUMN_PATTERN.findall(sql)


def read_simple_config(path: Path) -> dict:
    config: dict = {}
    stack: list[tuple[int, dict]] = [(-1, config)]
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, separator, value = raw_line.strip().partition(":")
        if not separator or not key:
            continue
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value.strip():
            parent[key.strip()] = value.strip()
        else:
            nested: dict = {}
            parent[key.strip()] = nested
            stack.append((indent, nested))
    return config


class QueryLevelMigrationTest(unittest.TestCase):
    def test_feature_columns_mirror_the_origin_table(self):
        origin = migration_columns(QUERY_ORIGIN)
        qid = migration_columns(QUERY_QID)

        self.assertEqual(
            [column for column in qid if column not in SERVICE_COLUMNS],
            origin,
        )

    def test_service_columns_are_present_right_after_the_key(self):
        columns = migration_columns(QUERY_QID)

        self.assertEqual(columns[:4], ["date", "query", "query_id", "has_query_id"])

    def test_hive_locks_are_disabled(self):
        sql = (QUERY_QID / "migrations" / "create_table.sql").read_text(encoding="utf-8")

        self.assertIn("'engine.hive.lock-enabled' = 'false'", sql)
        self.assertIn("PARTITIONED BY (date)", sql)


class QueryLevelConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = read_simple_config(QUERY_QID / "config.yaml")

    def test_table_identity(self):
        table = self.config["table"]

        self.assertEqual(table["catalog"], "iceberg")
        self.assertEqual(table["schema"], "gold")
        self.assertEqual(table["name"], "feature_platform_search_query_atc_features_qid")
        self.assertEqual(table["primary_key"], "date,query")
        self.assertEqual(table["meta"]["team"], "team:search")

    def test_primary_key_matches_layer_directory_group(self):
        self.assertEqual(QUERY_QID.parents[1].name, "query")

    def test_main_application_file_points_at_this_entity(self):
        self.assertEqual(
            self.config["spark"]["main_application_file"],
            "local:///git/repo/layers/gold/query/search_query_atc_features_qid/v1"
            "/entrypoints/get_search_query_atc_features_qid.py",
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 5: Запустить тест и убедиться, что он проходит**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: PASS, 6 тестов.

Если `test_feature_columns_mirror_the_origin_table` падает — значит при копировании миграции что-то поехало; сверить diff с оригиналом.

- [ ] **Step 6: Коммит**

```bash
git add layers/gold/query/search_query_atc_features_qid ci_test/test_query_id_features.py
git commit -m "feat(gold): add migration and config for search_query_atc_features_qid"
```

---

### Task 2: Таблица уровня запроса — job-код

**Files:**
- Create: `layers/gold/query/search_query_atc_features_qid/v1/job/__init__.py` (пустой)
- Create: `layers/gold/query/search_query_atc_features_qid/v1/job/entities.py`
- Create: `layers/gold/query/search_query_atc_features_qid/v1/job/arguments.py`
- Create: `layers/gold/query/search_query_atc_features_qid/v1/job/getting_search_query_atc_features_qid.py`
- Create: `layers/gold/query/search_query_atc_features_qid/v1/entrypoints/get_search_query_atc_features_qid.py`
- Modify: `ci_test/test_query_id_features.py`

**Interfaces:**
- Consumes: директорию и `config.yaml` из задачи 1.
- Produces: публичные имена в `job/getting_search_query_atc_features_qid.py`, на которые опираются тесты и задача 5:
  - `NORMALIZATION_REPLACEMENTS: tuple[tuple[str, str], ...]`
  - `QUERY_ID_TABLE: str`, `QUERY_ID_VERSION: str`
  - `WINDOWS: tuple[int, ...]`, `SELECTED_COLUMNS: tuple[str, ...]`
  - `normalize_query_value(value: str | None) -> str`
  - `normalize_query_column(column: Column) -> Column`
  - `parse_partition_date(partition_start: str) -> str`
  - `build_query_id_map(spark: SparkSession) -> DataFrame` — колонки `query, query_id`
  - `attach_group_key(frame: DataFrame, query_id_map: DataFrame) -> DataFrame` — добавляет `group_key`, `has_query_id`, убирает `query_id`
  - `run(spark: SparkSession, arguments: Arguments) -> None`

- [ ] **Step 1: Написать падающие тесты на чистые функции**

Дописать в `ci_test/test_query_id_features.py` перед блоком `if __name__`:

```python
import importlib.util
import sys
import types


def stub_pyspark() -> None:
    """Подменяет pyspark заглушками: локально его нет, а нам нужны только чистые функции."""
    if "pyspark" in sys.modules:
        return
    pyspark_module = types.ModuleType("pyspark")
    pyspark_sql_module = types.ModuleType("pyspark.sql")
    pyspark_sql_module.DataFrame = object
    pyspark_sql_module.SparkSession = object
    pyspark_sql_functions_module = types.ModuleType("pyspark.sql.functions")
    pyspark_sql_column_module = types.ModuleType("pyspark.sql.column")
    pyspark_sql_column_module.Column = object
    pyspark_sql_module.functions = pyspark_sql_functions_module
    pyspark_sql_module.column = pyspark_sql_column_module
    pyspark_module.sql = pyspark_sql_module
    sys.modules["pyspark"] = pyspark_module
    sys.modules["pyspark.sql"] = pyspark_sql_module
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions_module
    sys.modules["pyspark.sql.column"] = pyspark_sql_column_module


def load_job_module(entity_dir: Path, filename: str, module_name: str):
    stub_pyspark()
    entities_spec = importlib.util.spec_from_file_location(
        "job.entities",
        entity_dir / "job" / "entities.py",
    )
    entities_module = importlib.util.module_from_spec(entities_spec)
    job_package = types.ModuleType("job")
    job_package.__path__ = [str(entity_dir / "job")]
    sys.modules["job"] = job_package
    sys.modules["job.entities"] = entities_module
    entities_spec.loader.exec_module(entities_module)

    spec = importlib.util.spec_from_file_location(
        module_name,
        entity_dir / "job" / filename,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class QueryLevelJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module(
            QUERY_QID,
            "getting_search_query_atc_features_qid.py",
            "test_query_atc_features_qid_job",
        )

    def test_normalization_lowercases_folds_yo_and_collapses_spaces(self):
        self.assertEqual(self.job.normalize_query_value("  Красные   КРОССОВКИ "), "красные кроссовки")
        self.assertEqual(self.job.normalize_query_value("ЁЛКА"), "елка")
        self.assertEqual(self.job.normalize_query_value("ёлка"), "елка")

    def test_normalization_of_blank_input_is_empty(self):
        for value in (None, "", "   ", "\t\n"):
            with self.subTest(value=value):
                self.assertEqual(self.job.normalize_query_value(value), "")

    def test_supported_partition_date_formats(self):
        for value in (
            "2026-06-17 00:00:00",
            "2026-06-17T00:00:00",
            "2026-06-17T00:00:00+00:00",
            "2026-06-17T00:00:00Z",
            "2026-06-17 00:00:00+00:00",
        ):
            with self.subTest(value=value):
                self.assertEqual(self.job.parse_partition_date(value), "2026-06-17")

    def test_unsupported_partition_date_raises_with_value(self):
        with self.assertRaises(ValueError) as error:
            self.job.parse_partition_date("17.06.2026")

        self.assertIn("17.06.2026", str(error.exception))

    def test_dictionary_source_is_pinned_to_v1(self):
        self.assertEqual(
            self.job.QUERY_ID_TABLE,
            "iceberg.gold.feature_platform_search_query_id",
        )
        self.assertEqual(self.job.QUERY_ID_VERSION, "v1")

    def test_selected_columns_match_migration_columns(self):
        self.assertEqual(list(self.job.SELECTED_COLUMNS), migration_columns(QUERY_QID))

    def test_windows_mirror_the_origin_job(self):
        origin_source = (
            QUERY_ORIGIN / "job" / "getting_search_query_atc_features.py"
        ).read_text(encoding="utf-8")

        self.assertIn("WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)", origin_source)
        self.assertEqual(self.job.WINDOWS, (1, 3, 7, 14, 21, 30, 60, 90))
```

- [ ] **Step 2: Запустить тесты и убедиться, что они падают**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: FAIL — `FileNotFoundError` на `job/entities.py`, потому что job-кода ещё нет.

- [ ] **Step 3: Написать entities.py и arguments.py**

`layers/gold/query/search_query_atc_features_qid/v1/job/entities.py`:

```python
from dataclasses import dataclass


@dataclass
class Arguments:
    partition_start: str
    partition_end: str
    table_name: str
```

`layers/gold/query/search_query_atc_features_qid/v1/job/arguments.py`:

```python
from argparse import ArgumentParser

from job.entities import Arguments


def parse_arguments() -> Arguments:
    parser = ArgumentParser()
    parser.add_argument("--partition_start", type=str, required=True)
    parser.add_argument("--partition_end", type=str, required=True)
    parser.add_argument("--table_name", type=str, required=True)

    namespace, _ = parser.parse_known_args()

    return Arguments(
        partition_start=namespace.partition_start,
        partition_end=namespace.partition_end,
        table_name=namespace.table_name,
    )
```

И пустой `layers/gold/query/search_query_atc_features_qid/v1/job/__init__.py`.

- [ ] **Step 4: Написать job-код**

`layers/gold/query/search_query_atc_features_qid/v1/job/getting_search_query_atc_features_qid.py`:

```python
import re
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)

QUERY_ID_TABLE = "iceberg.gold.feature_platform_search_query_id"
QUERY_ID_VERSION = "v1"
NORMALIZATION_REPLACEMENTS = (("ё", "е"), (r"\s+", " "))

SELECTED_COLUMNS = (
    "date",
    "query",
    "query_id",
    "has_query_id",
    "query_uniq_impressions_1",
    "query_uniq_atcs_1",
    "query_orders_1",
    "query_uniq_impressions_3",
    "query_uniq_atcs_3",
    "query_orders_3",
    "query_uniq_impressions_7",
    "query_uniq_atcs_7",
    "query_orders_7",
    "query_uniq_impressions_14",
    "query_uniq_atcs_14",
    "query_orders_14",
    "query_uniq_impressions_21",
    "query_uniq_atcs_21",
    "query_orders_21",
    "query_uniq_impressions_30",
    "query_uniq_atcs_30",
    "query_orders_30",
    "query_uniq_impressions_60",
    "query_uniq_atcs_60",
    "query_orders_60",
    "query_uniq_impressions_90",
    "query_uniq_atcs_90",
    "query_orders_90",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def parse_partition_date(partition_start: str) -> str:
    supported_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    )
    normalized_value = partition_start
    if normalized_value.endswith("Z"):
        normalized_value = f"{normalized_value[:-1]}+0000"
    else:
        normalized_value = normalized_value.replace("+00:00", "+0000")

    for date_format in supported_formats:
        try:
            return datetime.strptime(normalized_value, date_format).date().isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(partition_start).date().isoformat()
    except ValueError as error:
        raise ValueError(
            "Unsupported partition_start value for search_query_atc_features_qid: "
            f"{partition_start}"
        ) from error


def normalize_query_value(value: str | None) -> str:
    """Чистый двойник normalize_query_column: та же цепочка шагов, но для тестов."""
    if value is None:
        return ""
    normalized = value.lower()
    for pattern, replacement in NORMALIZATION_REPLACEMENTS:
        normalized = re.sub(pattern, replacement, normalized)
    return normalized.strip()


def normalize_query_column(column: Column) -> Column:
    normalized = F.lower(column)
    for pattern, replacement in NORMALIZATION_REPLACEMENTS:
        normalized = F.regexp_replace(normalized, pattern, replacement)
    return F.trim(normalized)


def _normalize_query_frame(frame: DataFrame) -> DataFrame:
    return frame.withColumn("query", normalize_query_column(F.col("query"))).filter(
        F.col("query").isNotNull() & F.col("query").rlike(r"\S")
    )


def build_query_id_map(spark: SparkSession) -> DataFrame:
    """Справочник query -> query_id, схлопнутый до одной строки на нормализованный запрос.

    Справочник хранит сырой query_text, поэтому нормализация обязательна. PK там задан на
    сыром тексте, так что несколько сырых вариантов схлопываются в один нормализованный;
    min даёт детерминированный результат при перезапуске.
    """
    return (
        spark.table(QUERY_ID_TABLE)
        .filter(F.col("version") == F.lit(QUERY_ID_VERSION))
        .select(
            normalize_query_column(F.col("query_text")).alias("query"),
            F.trim(F.lower(F.col("query_id"))).alias("query_id"),
        )
        .filter(F.col("query").rlike(r"\S") & F.col("query_id").rlike(r"\S"))
        .groupBy("query")
        .agg(F.min("query_id").alias("query_id"))
    )


def attach_group_key(frame: DataFrame, query_id_map: DataFrame) -> DataFrame:
    """Ключ агрегации: каноничный query_id, либо сам запрос, если его нет в справочнике."""
    return (
        frame.join(query_id_map, on="query", how="left")
        .withColumn("has_query_id", F.col("query_id").isNotNull())
        .withColumn("group_key", F.coalesce(F.col("query_id"), F.col("query")))
        .drop("query_id")
    )


def _window_start_dates(run_date: str) -> dict[int, str]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return {
        window: (run_dt - timedelta(days=window)).isoformat()
        for window in WINDOWS
    }


def _sum_between(
    column_name: str,
    start_date: str,
    finish_date_exclusive: str,
) -> Column:
    return F.sum(
        F.when(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(finish_date_exclusive).cast("date")),
            F.col(column_name),
        ).otherwise(0.0)
    )


def _build_window_features(
    events: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    aggregations = []
    for window in WINDOWS:
        aggregations.extend(
            (
                _sum_between("sum_impressions", window_dates[window], run_date).alias(
                    f"query_uniq_impressions_{window}"
                ),
                _sum_between("sum_atc", window_dates[window], run_date).alias(
                    f"query_uniq_atcs_{window}"
                ),
            )
        )

    return events.groupBy("group_key").agg(*aggregations)


def _build_order_window_features(
    orders: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    return orders.groupBy("group_key").agg(
        *[
            _sum_between("orders_generated", window_dates[window], run_date).alias(
                f"query_orders_{window}"
            )
            for window in WINDOWS
        ]
    )


def build_search_query_atc_features_qid(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    window_dates = _window_start_dates(run_date)
    start_date = window_dates[max(WINDOWS)]

    query_id_map = build_query_id_map(spark)

    events = _normalize_query_frame(
        spark.table("iceberg.silver.feature_platform_search_sku_group_id_install_query")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(run_date).cast("date"))
        )
        .filter(F.col("space") == F.lit("SEARCH_RESULTS"))
        .select(
            F.col("date"),
            F.col("uniqs").alias("query"),
            F.col("sum_impressions").cast("double").alias("sum_impressions"),
            F.col("sum_atc").cast("double").alias("sum_atc"),
        )
    )

    orders = _normalize_query_frame(
        spark.table("iceberg.silver.feature_platform_sku_group_query_search_orders")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(run_date).cast("date"))
        )
        .select(
            F.col("date"),
            F.col("query"),
            F.col("orders_generated").cast("double").alias("orders_generated"),
        )
    )

    members = attach_group_key(events.select("query").distinct(), query_id_map)

    features = _build_window_features(
        attach_group_key(events, query_id_map),
        window_dates,
        run_date,
    ).join(
        _build_order_window_features(
            attach_group_key(orders, query_id_map),
            window_dates,
            run_date,
        ),
        on="group_key",
        how="left",
    )

    for window in WINDOWS:
        features = features.withColumn(
            f"query_orders_{window}",
            F.coalesce(F.col(f"query_orders_{window}"), F.lit(0.0)),
        )

    return (
        members.join(features, on="group_key", how="inner")
        .withColumn("date", F.lit(run_date).cast("date"))
        .withColumnRenamed("group_key", "query_id")
        .select(*SELECTED_COLUMNS)
    )


def save_search_query_atc_features_qid(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_search_query_atc_features_qid(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_search_query_atc_features_qid(
        spark,
        parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
```

- [ ] **Step 5: Написать entrypoint**

`layers/gold/query/search_query_atc_features_qid/v1/entrypoints/get_search_query_atc_features_qid.py`:

```python
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from job.arguments import parse_arguments
from job.getting_search_query_atc_features_qid import run


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("getting-search-query-atc-features-qid")
        .enableHiveSupport()
        .getOrCreate()
    )
    arguments = parse_arguments()

    try:
        run(spark, arguments)
    finally:
        spark.stop()
```

- [ ] **Step 6: Запустить тесты и убедиться, что они проходят**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: PASS, 13 тестов.

- [ ] **Step 7: Коммит**

```bash
git add layers/gold/query/search_query_atc_features_qid ci_test/test_query_id_features.py
git commit -m "feat(gold): aggregate search_query_atc_features_qid within query_id"
```

---

### Task 3: Таблица уровня запроса — DAG и README

**Files:**
- Create: `layers/gold/query/search_query_atc_features_qid/v1/dag.py`
- Create: `layers/gold/query/search_query_atc_features_qid/v1/README.md`
- Modify: `ci_test/test_query_id_features.py`

**Interfaces:**
- Consumes: `config.yaml` и entrypoint из задач 1–2.
- Produces: DAG `feature-platform.layers.gold.query.search_query_atc_features_qid` с расписанием `0 6 * * *` UTC. Этот dag_id и крон используются тестом дельт в задаче 7.

- [ ] **Step 1: Написать тест на оркестрацию**

Дописать в `ci_test/test_query_id_features.py`:

```python
DAG_ID_PATTERN = re.compile(r'dag_id="([^"]+)"')
CRON_PATTERN = re.compile(r"CronDataIntervalTimetable\(\s*['\"]([^'\"]+)['\"]")


class QueryLevelDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (QUERY_QID / "dag.py").read_text(encoding="utf-8")

    def test_dag_id_encodes_repository_path(self):
        self.assertEqual(
            DAG_ID_PATTERN.search(self.source).group(1),
            "feature-platform.layers.gold.query.search_query_atc_features_qid",
        )

    def test_schedule_runs_after_the_query_id_dag(self):
        self.assertEqual(CRON_PATTERN.search(self.source).group(1), "0 6 * * *")

    def test_it_waits_for_the_query_id_dag_itself_not_its_dq(self):
        self.assertIn(
            '"feature-platform.layers.gold.query_text_version.search_query_id"',
            self.source,
        )
        self.assertNotIn(
            "ml_feature_platform_gold.feature_platform_search_query_id.dq",
            self.source,
        )

    def test_it_waits_for_both_silver_dq_dags(self):
        self.assertIn(
            "feature_platform_search_sku_group_id_install_query.dq",
            self.source,
        )
        self.assertIn(
            "feature_platform_sku_group_query_search_orders.dq",
            self.source,
        )

    def test_execution_deltas_line_up_with_the_schedule(self):
        # 06:00 логической даты минус 1 час = 05:00, логическая дата прогона search_query_id.
        # 06:00 минус 5 часов = 01:00, логическая дата DQ силверов.
        self.assertIn("execution_delta=timedelta(hours=1)", self.source)
        self.assertIn("execution_delta=timedelta(hours=5)", self.source)

    def test_dag_is_paused_upon_creation(self):
        self.assertIn("is_paused_upon_creation=True", self.source)
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: FAIL — `FileNotFoundError` на `dag.py`.

- [ ] **Step 3: Написать DAG**

`layers/gold/query/search_query_atc_features_qid/v1/dag.py`:

```python
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

from config.factory import get_dag_settings, get_deployment
from airflow.sdk import dag
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.timetables.interval import CronDataIntervalTimetable

dag_settings = get_dag_settings()

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")

default_args = {
    "owner": dag_settings["owner"],
    "depends_on_past": False,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": send_oncall_notification(
        severity=dag_settings["alert_severity"],
        team=dag_settings["alert_team"],
        oncall_webhook_conn_id=dag_settings["alert_oncall_webhook_conn_id"],
    ),
}


@dag(
    default_args=default_args,
    max_active_runs=1,
    tags=["spark", "feature-platform", dag_settings["team_tag"], "gold", "query", "query-id"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable("0 6 * * *", "UTC"),
    start_date=datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc),
    dag_id="feature-platform.layers.gold.query.search_query_atc_features_qid",
)
def collect_gold_search_query_atc_features_qid():
    wait_for_query_id = ExternalTaskSensor(
        task_id="wait_for_search_query_id",
        external_dag_id="feature-platform.layers.gold.query_text_version.search_query_id",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(hours=1),
    )

    wait_for_silver_install_stats = ExternalTaskSensor(
        task_id="wait_for_silver_sku_group_install_stats",
        external_dag_id=(
            "dbt.source.trino.ml_feature_platform_silver."
            "feature_platform_search_sku_group_id_install_query.dq"
        ),
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(hours=5),
    )

    wait_for_silver_search_orders = ExternalTaskSensor(
        task_id="wait_for_silver_sku_group_query_search_orders",
        external_dag_id=(
            "dbt.source.trino.ml_feature_platform_silver."
            "feature_platform_sku_group_query_search_orders.dq"
        ),
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(hours=5),
    )

    collect_features = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_search_query_atc_features_qid",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_search_query_atc_features_qid.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    [
        wait_for_query_id,
        wait_for_silver_install_stats,
        wait_for_silver_search_orders,
    ] >> collect_features


dag = collect_gold_search_query_atc_features_qid()
```

Имя `fetch_gold_search_query_atc_features_qid.yaml` локально не существует — `get_deployment` в этом случае падает обратно на shared-шаблон из `spark.template_path`. Это штатный путь, так же устроены остальные сущности.

- [ ] **Step 4: Написать README**

`layers/gold/query/search_query_atc_features_qid/v1/README.md`:

```markdown
# iceberg.gold.feature_platform_search_query_atc_features_qid

Фичи показов, ATC и заказов по поисковому запросу, посчитанные в рамках каноничного `query_id`
и развёрнутые обратно на исходные `query_text`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_search_query_atc_features_qid`.
- DAG: `feature-platform.layers.gold.query.search_query_atc_features_qid`
  (`layers/gold/query/search_query_atc_features_qid/v1/dag.py`).
- Расписание: ежедневно, `0 6 * * *` UTC, после DAG справочника `query_id` (`0 5 * * *`).
- `start_date=2026-08-11T00:00:00Z`, `is_paused_upon_creation=True`.

## Грейн / ключ

`date, query`. `query` — исходный нормализованный поисковый запрос, а не `query_id`: ключ джойна
в ranking-сервисе не меняется.

## Источники

- `iceberg.silver.feature_platform_search_sku_group_id_install_query` - показы и ATC, колонка
  `uniqs` при `space = 'SEARCH_RESULTS'`.
- `iceberg.silver.feature_platform_sku_group_query_search_orders` - сгенерированные заказы.
- `iceberg.gold.feature_platform_search_query_id` - справочник каноничных `query_id`, `version = 'v1'`.

## Зависимости

- `feature-platform.layers.gold.query_text_version.search_query_id` (`execution_delta = 1 час`) -
  сам DAG справочника, не его DQ.
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`
  (`execution_delta = 5 часов`).
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq`
  (`execution_delta = 5 часов`).

## Логика

Отличие от `feature_platform_search_query_atc_features` ровно одно: ключ агрегации. Имена фичей,
формулы, окна (1, 3, 7, 14, 21, 30, 60, 90) и границы окон `[ds - N, ds - 1]` совпадают дословно.

1. Запрос нормализуется: `lower` -> `ё` в `е` -> схлопывание пробелов -> `trim`. Та же нормализация
   применяется к `query_text` из справочника, потому что справочник хранит сырой текст.
2. `group_key = coalesce(query_id, query)`. Запрос, которого нет в справочнике, образует группу из
   самого себя, поэтому покрытие выхода не теряется.
3. Оконные суммы считаются по `group_key`.
4. Результат разворачивается обратно: каждый `query_text` группы получает значения своей группы.

Число строк совпадает с `feature_platform_search_query_atc_features`: универсум запросов тот же,
меняются только значения.

## Колонки сверх зеркала

- `query_id` - ключ группы, по которой посчитана строка. При фолбэке равен самому `query`.
- `has_query_id` - `false` для фолбэка.

Обе колонки служебные, в вектор ranking upload не входят.

## Рантайм

Shared Spark-образ и `git-sync`, шаблон `config/spark/layer_spark_application.yaml`, профиль
ресурсов `small`. Запись - `overwritePartitions()` по `date`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
```

- [ ] **Step 5: Запустить тесты и общие проверки репозитория**

Run:
```bash
python3 ci_test/test_query_id_features.py -v
python3 ci_test/test_script.py
```
Expected: оба PASS. `test_script.py` проверяет структуру директорий, наличие `TBLPROPERTIES`, соответствие группы первичного ключа имени директории и валидность markdown-ссылок в README.

- [ ] **Step 6: Коммит**

```bash
git add layers/gold/query/search_query_atc_features_qid ci_test/test_query_id_features.py
git commit -m "feat(gold): add DAG and README for search_query_atc_features_qid"
```

---

### Task 4: Парная таблица — миграция и config

**Files:**
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/migrations/create_table.sql`
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/migrations/__init__.py` (пустой)
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/config.yaml`
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/config/factory.py`
- Modify: `ci_test/test_query_id_features.py`

**Interfaces:**
- Consumes: ничего от предыдущих задач.
- Produces: таблица `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_qid` с колонками `date, query, sku_group_id, query_id, has_query_id` + 61 фичевая колонка, идентичная v2.

- [ ] **Step 1: Создать директории и скопировать основу**

```bash
mkdir -p layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/migrations
mkdir -p layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/config
mkdir -p layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/job
mkdir -p layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/entrypoints
touch layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/migrations/__init__.py
cp layers/gold/query_sku_group_id/sku_group_query_atc_order_features/v2/migrations/create_table.sql \
   layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/migrations/create_table.sql
cp layers/gold/query_sku_group_id/sku_group_query_atc_order_features/v2/config/factory.py \
   layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/config/factory.py
```

- [ ] **Step 2: Добавить служебные колонки в миграцию**

Заменить строку

```sql
    sku_group_id BIGINT COMMENT 'ID sku group',
```

на

```sql
    sku_group_id BIGINT COMMENT 'ID sku group',
    query_id STRING COMMENT 'Ключ группы, по которой посчитаны значения строки: каноничный query_id из feature_platform_search_query_id либо сам нормализованный query при отсутствии в справочнике',
    has_query_id BOOLEAN COMMENT 'true, если query нашёлся в справочнике feature_platform_search_query_id; false для фолбэка на сам запрос',
```

и заменить строку

```sql
COMMENT 'Gold-фичи ATC и заказных конверсий по query и sku_group_id'
```

на

```sql
COMMENT 'Gold-фичи ATC и заказных конверсий по query и sku_group_id, агрегированные в рамках query_id'
```

- [ ] **Step 3: Написать config.yaml**

```yaml
resources:
  path: ../../../../../config/spark/resources.yaml

spark:
  template_path: ../../../../../config/spark/layer_spark_application.yaml
  application_name: fetch-gold-sku-group-query-atc-order-features-qid
  main_application_file: local:///git/repo/layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/entrypoints/get_sku_group_query_atc_order_features_qid.py
  resource_profile: large

table:
  key: sku_group_query_atc_order_features_qid
  catalog: iceberg
  schema: gold
  name: feature_platform_search_sku_group_id_query_atc_order_features_qid
  primary_key: date,query,sku_group_id
  meta:
    team: team:search
dag:
  team: search

alerts:
  team: search
  severity: P3
  oncall_webhook_conn_id: oncall_webhook_search
```

- [ ] **Step 4: Написать тесты на миграцию и config парной таблицы**

Дописать в `ci_test/test_query_id_features.py` (константы — рядом с `QUERY_QID` в начале файла):

```python
PAIR_ORIGIN = (
    ROOT / "layers" / "gold" / "query_sku_group_id"
    / "sku_group_query_atc_order_features" / "v2"
)
PAIR_QID = (
    ROOT / "layers" / "gold" / "query_sku_group_id"
    / "sku_group_query_atc_order_features_qid" / "v1"
)
```

и класс:

```python
class PairMigrationAndConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = read_simple_config(PAIR_QID / "config.yaml")

    def test_feature_columns_mirror_the_origin_table(self):
        origin = migration_columns(PAIR_ORIGIN)
        qid = migration_columns(PAIR_QID)

        self.assertEqual(
            [column for column in qid if column not in SERVICE_COLUMNS],
            origin,
        )

    def test_service_columns_are_present_right_after_the_key(self):
        columns = migration_columns(PAIR_QID)

        self.assertEqual(
            columns[:5],
            ["date", "query", "sku_group_id", "query_id", "has_query_id"],
        )

    def test_hive_locks_are_disabled(self):
        sql = (PAIR_QID / "migrations" / "create_table.sql").read_text(encoding="utf-8")

        self.assertIn("'engine.hive.lock-enabled' = 'false'", sql)
        self.assertIn("PARTITIONED BY (date)", sql)

    def test_table_identity(self):
        table = self.config["table"]

        self.assertEqual(table["catalog"], "iceberg")
        self.assertEqual(table["schema"], "gold")
        self.assertEqual(
            table["name"],
            "feature_platform_search_sku_group_id_query_atc_order_features_qid",
        )
        self.assertEqual(table["primary_key"], "date,query,sku_group_id")
        self.assertEqual(table["meta"]["team"], "team:search")

    def test_primary_key_matches_layer_directory_group(self):
        self.assertEqual(PAIR_QID.parents[1].name, "query_sku_group_id")

    def test_resource_profile_matches_the_origin(self):
        origin_config = read_simple_config(PAIR_ORIGIN / "config.yaml")

        self.assertEqual(
            self.config["spark"]["resource_profile"],
            origin_config["spark"]["resource_profile"],
        )
```

- [ ] **Step 5: Запустить тесты**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: PASS, 25 тестов.

- [ ] **Step 6: Коммит**

```bash
git add layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid ci_test/test_query_id_features.py
git commit -m "feat(gold): add migration and config for sku_group_query_atc_order_features_qid"
```

---

### Task 5: Парная таблица — job-код

**Files:**
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/job/__init__.py` (пустой)
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/job/entities.py`
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/job/arguments.py`
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/job/getting_sku_group_query_atc_order_features_qid.py`
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/entrypoints/get_sku_group_query_atc_order_features_qid.py`
- Modify: `ci_test/test_query_id_features.py`

**Interfaces:**
- Consumes: директорию и `config.yaml` из задачи 4; helper-функции тестов `load_job_module`, `migration_columns` из задач 1–2.
- Produces: те же публичные имена, что в задаче 2 (`NORMALIZATION_REPLACEMENTS`, `QUERY_ID_TABLE`, `QUERY_ID_VERSION`, `WINDOWS`, `SELECTED_COLUMNS`, `normalize_query_value`, `normalize_query_column`, `parse_partition_date`, `build_query_id_map`, `attach_group_key`, `run`), плюс `SMOOTHING_COEF: float`.

- [ ] **Step 1: Написать падающие тесты**

Дописать в `ci_test/test_query_id_features.py`:

```python
class PairJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module(
            PAIR_QID,
            "getting_sku_group_query_atc_order_features_qid.py",
            "test_sku_group_query_atc_order_features_qid_job",
        )

    def test_selected_columns_match_migration_columns(self):
        self.assertEqual(list(self.job.SELECTED_COLUMNS), migration_columns(PAIR_QID))

    def test_smoothing_coefficient_mirrors_the_origin_job(self):
        origin_source = (
            PAIR_ORIGIN / "job" / "getting_sku_group_query_atc_order_features.py"
        ).read_text(encoding="utf-8")

        self.assertIn("SMOOTHING_COEF = 100.0", origin_source)
        self.assertEqual(self.job.SMOOTHING_COEF, 100.0)

    def test_windows_mirror_the_origin_job(self):
        self.assertEqual(self.job.WINDOWS, (1, 3, 7, 14, 21, 30, 60, 90))

    def test_normalization_is_identical_to_the_query_level_job(self):
        query_job = load_job_module(
            QUERY_QID,
            "getting_search_query_atc_features_qid.py",
            "test_query_atc_features_qid_job_for_pair",
        )

        self.assertEqual(
            self.job.NORMALIZATION_REPLACEMENTS,
            query_job.NORMALIZATION_REPLACEMENTS,
        )
        for value in ("  Ёлка   ЗЕЛЁНАЯ ", "krossovka", ""):
            with self.subTest(value=value):
                self.assertEqual(
                    self.job.normalize_query_value(value),
                    query_job.normalize_query_value(value),
                )

    def test_dictionary_source_is_pinned_to_v1(self):
        self.assertEqual(
            self.job.QUERY_ID_TABLE,
            "iceberg.gold.feature_platform_search_query_id",
        )
        self.assertEqual(self.job.QUERY_ID_VERSION, "v1")

    def test_partition_date_is_parsed_not_sliced(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("partition_start[:10]", source)
        self.assertEqual(self.job.parse_partition_date("2026-06-17T00:00:00Z"), "2026-06-17")

    def test_sku_level_denominators_stay_grouped_by_sku_group_id_only(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertIn('daily_events.groupBy("sku_group_id")', source)
        self.assertIn('orders.groupBy("sku_group_id")', source)

    def test_pair_aggregations_are_grouped_by_group_key(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertIn('groupBy("group_key", "sku_group_id")', source)
        self.assertNotIn('groupBy("query", "sku_group_id")', source)
```

- [ ] **Step 2: Запустить тесты и убедиться, что они падают**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: FAIL — `FileNotFoundError` на `job/entities.py` парной сущности.

- [ ] **Step 3: Написать entities.py, arguments.py и __init__.py**

`job/entities.py`:

```python
from dataclasses import dataclass


@dataclass
class Arguments:
    partition_start: str
    partition_end: str
    table_name: str
```

`job/arguments.py`:

```python
from argparse import ArgumentParser

from job.entities import Arguments


def parse_arguments() -> Arguments:
    parser = ArgumentParser()
    parser.add_argument("--partition_start", type=str, required=True)
    parser.add_argument("--partition_end", type=str, required=True)
    parser.add_argument("--table_name", type=str, required=True)

    namespace, _ = parser.parse_known_args()

    return Arguments(
        partition_start=namespace.partition_start,
        partition_end=namespace.partition_end,
        table_name=namespace.table_name,
    )
```

И пустой `job/__init__.py`.

- [ ] **Step 4: Скопировать job-код v2 как основу**

```bash
cp layers/gold/query_sku_group_id/sku_group_query_atc_order_features/v2/job/getting_sku_group_query_atc_order_features.py \
   layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/job/getting_sku_group_query_atc_order_features_qid.py
```

Дальше правится только этот новый файл.

- [ ] **Step 5: Добавить константы, нормализацию и разбор даты**

Заменить блок импортов и шапку до `SELECTED_COLUMNS`:

```python
import re
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql import functions as F

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)
SMOOTHING_COEF = 100.0

QUERY_ID_TABLE = "iceberg.gold.feature_platform_search_query_id"
QUERY_ID_VERSION = "v1"
NORMALIZATION_REPLACEMENTS = (("ё", "е"), (r"\s+", " "))
```

В кортеже `SELECTED_COLUMNS` заменить начало

```python
SELECTED_COLUMNS = (
    "date",
    "query",
    "sku_group_id",
```

на

```python
SELECTED_COLUMNS = (
    "date",
    "query",
    "sku_group_id",
    "query_id",
    "has_query_id",
```

Остальные 61 фичевая запись кортежа не трогается.

Заменить функцию `_normalize_query_frame` целиком на четыре функции:

```python
def parse_partition_date(partition_start: str) -> str:
    supported_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    )
    normalized_value = partition_start
    if normalized_value.endswith("Z"):
        normalized_value = f"{normalized_value[:-1]}+0000"
    else:
        normalized_value = normalized_value.replace("+00:00", "+0000")

    for date_format in supported_formats:
        try:
            return datetime.strptime(normalized_value, date_format).date().isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(partition_start).date().isoformat()
    except ValueError as error:
        raise ValueError(
            "Unsupported partition_start value for "
            f"sku_group_query_atc_order_features_qid: {partition_start}"
        ) from error


def normalize_query_value(value: str | None) -> str:
    """Чистый двойник normalize_query_column: та же цепочка шагов, но для тестов."""
    if value is None:
        return ""
    normalized = value.lower()
    for pattern, replacement in NORMALIZATION_REPLACEMENTS:
        normalized = re.sub(pattern, replacement, normalized)
    return normalized.strip()


def normalize_query_column(column: Column) -> Column:
    normalized = F.lower(column)
    for pattern, replacement in NORMALIZATION_REPLACEMENTS:
        normalized = F.regexp_replace(normalized, pattern, replacement)
    return F.trim(normalized)


def _normalize_query_frame(frame: DataFrame) -> DataFrame:
    return frame.withColumn("query", normalize_query_column(F.col("query"))).filter(
        F.col("query").isNotNull() & F.col("query").rlike(r"\S")
    )


def build_query_id_map(spark: SparkSession) -> DataFrame:
    """Справочник query -> query_id, схлопнутый до одной строки на нормализованный запрос.

    Справочник хранит сырой query_text, поэтому нормализация обязательна. PK там задан на
    сыром тексте, так что несколько сырых вариантов схлопываются в один нормализованный;
    min даёт детерминированный результат при перезапуске.
    """
    return (
        spark.table(QUERY_ID_TABLE)
        .filter(F.col("version") == F.lit(QUERY_ID_VERSION))
        .select(
            normalize_query_column(F.col("query_text")).alias("query"),
            F.trim(F.lower(F.col("query_id"))).alias("query_id"),
        )
        .filter(F.col("query").rlike(r"\S") & F.col("query_id").rlike(r"\S"))
        .groupBy("query")
        .agg(F.min("query_id").alias("query_id"))
    )


def attach_group_key(frame: DataFrame, query_id_map: DataFrame) -> DataFrame:
    """Ключ агрегации: каноничный query_id, либо сам запрос, если его нет в справочнике."""
    return (
        frame.join(query_id_map, on="query", how="left")
        .withColumn("has_query_id", F.col("query_id").isNotNull())
        .withColumn("group_key", F.coalesce(F.col("query_id"), F.col("query")))
        .drop("query_id")
    )
```

- [ ] **Step 6: Переключить парные агрегации на group_key**

В четырёх функциях заменить ключ группировки. `_build_events_agg`:

```python
    return events.groupBy("group_key", "sku_group_id").agg(*aggregations)
```

`_build_smoothed_pair_events_agg`:

```python
    return events.groupBy("group_key", "sku_group_id").agg(*aggregations)
```

`_build_smoothed_pair_orders_agg`:

```python
    return orders.groupBy("group_key", "sku_group_id").agg(
```

`_build_orders_agg`:

```python
    return orders.groupBy("group_key", "sku_group_id").agg(
```

Функции `_build_smoothed_skg_events_agg` и `_build_smoothed_skg_orders_agg` **не трогать**: знаменатели уровня sku не зависят от запроса, они и должны остаться на `groupBy("sku_group_id")`.

- [ ] **Step 7: Переписать build-функцию под группировку и разворот**

В `build_sku_group_query_atc_order_features` (переименовать в `build_sku_group_query_atc_order_features_qid`) после построения `orders` вставить карту справочника и заменить блок джойнов.

Сразу после `window_dates` и `d90` добавить:

```python
    query_id_map = build_query_id_map(spark)
```

После определения `events` и `orders` добавить:

```python
    grouped_events = attach_group_key(events, query_id_map)
    grouped_orders = attach_group_key(orders, query_id_map)
    members = attach_group_key(events.select("query").distinct(), query_id_map)
```

Заменить шесть вызовов агрегаторов так, чтобы парные брали сгруппированные фреймы, а sku-уровневые — исходные:

```python
    events_agg = _build_events_agg(grouped_events, window_dates)
    smoothed_pair_events_agg = _build_smoothed_pair_events_agg(
        grouped_events,
        window_dates,
        run_date,
    )
    smoothed_pair_orders_agg = _build_smoothed_pair_orders_agg(
        grouped_orders,
        window_dates,
        run_date,
    )
    smoothed_skg_events_agg = _build_smoothed_skg_events_agg(
        spark,
        window_dates[max(WINDOWS)],
        run_date,
        window_dates,
    )
    smoothed_skg_orders_agg = _build_smoothed_skg_orders_agg(
        orders,
        window_dates,
        run_date,
    )
    orders_agg = _build_orders_agg(grouped_orders, window_dates)
```

Заменить цепочку джойнов и заполнение нулями: ключ пар теперь `group_key`, а не `query`.

```python
    features = events_agg.join(orders_agg, on=["group_key", "sku_group_id"], how="left")
    features = features.join(
        smoothed_pair_events_agg,
        on=["group_key", "sku_group_id"],
        how="left",
    )
    features = features.join(
        smoothed_pair_orders_agg,
        on=["group_key", "sku_group_id"],
        how="left",
    )
    features = features.join(
        smoothed_skg_events_agg,
        on=["sku_group_id"],
        how="left",
    )
    features = features.join(
        smoothed_skg_orders_agg,
        on=["sku_group_id"],
        how="left",
    )
    for column_name in features.columns:
        if column_name not in ("group_key", "sku_group_id"):
            features = features.withColumn(column_name, F.coalesce(F.col(column_name), F.lit(0.0)))
```

Фильтры оставить как есть — они применяются к `features`, то есть уже на уровне группы, до разворота:

```python
    features = features.filter(F.col("query_skg_uniq_impressions_14") >= F.lit(2.0))
    features = features.filter(
        (F.col("query_skg_uniq_atcs_90") > F.lit(0.0))
        | (F.col("query_skg_uniq_orders_90") > F.lit(0.0))
    )
```

Все блоки `for window in WINDOWS:` с производными конверсиями и все `withColumn` с отношениями окон оставить без изменений.

Изменить хвост функции. В оригинале последняя цепочка `withColumn` заканчивается так:

```python
        .withColumn(
            "query_skg_imp2order_90_to_60",
            _safe_div(F.col("query_skg_conv_imp2order_90"), F.col("query_skg_conv_imp2order_60")),
        )
        .withColumn("date", F.lit(run_date).cast("date"))
    )

    return features.select(*SELECTED_COLUMNS)
```

Заменить на:

```python
        .withColumn(
            "query_skg_imp2order_90_to_60",
            _safe_div(F.col("query_skg_conv_imp2order_90"), F.col("query_skg_conv_imp2order_60")),
        )
    )

    return (
        members.join(features, on="group_key", how="inner")
        .withColumn("date", F.lit(run_date).cast("date"))
        .withColumnRenamed("group_key", "query_id")
        .select(*SELECTED_COLUMNS)
    )
```

То есть `.withColumn("date", ...)` убирается из цепочки, а разворот через `members`, простановка
даты и переименование `group_key` в `query_id` уезжают в `return`. Порядок важен: дата ставится
после разворота, иначе она попадёт в join-ключ.

На выходе `members` даёт колонки `query`, `group_key`, `has_query_id`, а `features` —
`group_key`, `sku_group_id` и все фичевые колонки, так что после join и переименования
в кадре есть ровно то, что перечислено в `SELECTED_COLUMNS`.

- [ ] **Step 8: Обновить save и run**

```python
def save_sku_group_query_atc_order_features_qid(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_query_atc_order_features_qid(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_query_atc_order_features_qid(
        spark,
        parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
```

- [ ] **Step 9: Написать entrypoint**

`layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/entrypoints/get_sku_group_query_atc_order_features_qid.py`:

```python
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from job.arguments import parse_arguments
from job.getting_sku_group_query_atc_order_features_qid import run


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("getting-sku-group-query-atc-order-features-qid")
        .enableHiveSupport()
        .getOrCreate()
    )
    arguments = parse_arguments()

    try:
        run(spark, arguments)
    finally:
        spark.stop()
```

- [ ] **Step 10: Запустить тесты**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: PASS, 33 теста.

Если `test_pair_aggregations_are_grouped_by_group_key` падает — остался незамененный `groupBy("query", "sku_group_id")` в одном из четырёх парных агрегаторов.

- [ ] **Step 11: Коммит**

```bash
git add layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid ci_test/test_query_id_features.py
git commit -m "feat(gold): aggregate sku_group_query_atc_order_features_qid within query_id"
```

---

### Task 6: Парная таблица — DAG и README

**Files:**
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/dag.py`
- Create: `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/README.md`
- Modify: `ci_test/test_query_id_features.py`

**Interfaces:**
- Consumes: `config.yaml` и entrypoint из задач 4–5.
- Produces: DAG `feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features_qid`, расписание `0 6 * * *` UTC.

- [ ] **Step 1: Написать тест на оркестрацию парного DAG**

Дописать в `ci_test/test_query_id_features.py`:

```python
class PairDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (PAIR_QID / "dag.py").read_text(encoding="utf-8")

    def test_dag_id_encodes_repository_path(self):
        self.assertEqual(
            DAG_ID_PATTERN.search(self.source).group(1),
            "feature-platform.layers.gold.query_sku_group_id."
            "sku_group_query_atc_order_features_qid",
        )

    def test_schedule_runs_after_the_query_id_dag(self):
        self.assertEqual(CRON_PATTERN.search(self.source).group(1), "0 6 * * *")

    def test_it_waits_for_the_query_id_dag_itself(self):
        self.assertIn(
            '"feature-platform.layers.gold.query_text_version.search_query_id"',
            self.source,
        )
        self.assertIn("execution_delta=timedelta(hours=1)", self.source)

    def test_it_waits_for_both_silver_dq_dags(self):
        self.assertIn("feature_platform_search_sku_group_id_install_query.dq", self.source)
        self.assertIn("feature_platform_sku_group_query_search_orders.dq", self.source)
        self.assertIn("execution_delta=timedelta(hours=5)", self.source)

    def test_dag_is_paused_upon_creation(self):
        self.assertIn("is_paused_upon_creation=True", self.source)
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 ci_test/test_query_id_features.py -v`
Expected: FAIL — `FileNotFoundError` на `dag.py`.

- [ ] **Step 3: Написать DAG**

Скопировать DAG из задачи 3 и изменить пять мест:

```bash
cp layers/gold/query/search_query_atc_features_qid/v1/dag.py \
   layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/dag.py
```

Затем в новом файле:

1. в `tags` заменить `"query", "query-id"` на `"orders", "atc", "query-id"`;
2. `dag_id` заменить на
   `"feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features_qid"`;
3. имя функции заменить с `collect_gold_search_query_atc_features_qid` на
   `collect_gold_sku_group_query_atc_order_features_qid` (в двух местах: определение и вызов внизу файла);
4. `task_id` Spark-оператора заменить на `"getting_sku_group_query_atc_order_features_qid"`;
5. имя файла деплоя заменить на `"fetch_gold_sku_group_query_atc_order_features_qid.yaml"`.

Расписание, `start_date`, все три сенсора и их `execution_delta` остаются теми же.

- [ ] **Step 4: Написать README**

`layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/README.md`:

```markdown
# iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_qid

Pairwise-фичи ATC и заказных конверсий по паре запрос-sku_group_id, посчитанные в рамках
каноничного `query_id` и развёрнутые обратно на исходные `query_text`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_qid`.
- DAG: `feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features_qid`
  (`layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/dag.py`).
- Расписание: ежедневно, `0 6 * * *` UTC, после DAG справочника `query_id` (`0 5 * * *`).
- `start_date=2026-08-11T00:00:00Z`, `is_paused_upon_creation=True`.

## Грейн / ключ

`date, query, sku_group_id`. `query` - исходный нормализованный поисковый запрос, а не `query_id`.

## Источники

- `iceberg.silver.feature_platform_search_sku_group_id_install_query` - показы и ATC при
  `space = 'SEARCH_RESULTS'`.
- `iceberg.silver.feature_platform_sku_group_query_search_orders` - сгенерированные заказы.
- `iceberg.gold.feature_platform_search_query_id` - справочник каноничных `query_id`, `version = 'v1'`.

## Зависимости

- `feature-platform.layers.gold.query_text_version.search_query_id` (`execution_delta = 1 час`) -
  сам DAG справочника, не его DQ.
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`
  (`execution_delta = 5 часов`).
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq`
  (`execution_delta = 5 часов`).

## Логика

Отличие от `feature_platform_search_sku_group_id_query_atc_order_features_v2` ровно одно: ключ
парной агрегации. Имена фичей, формулы, окна (1, 3, 7, 14, 21, 30, 60, 90), коэффициент сглаживания
100 и границы окон совпадают дословно.

1. Запрос нормализуется: `lower` -> `ё` в `е` -> схлопывание пробелов -> `trim`. Та же нормализация
   применяется к `query_text` из справочника.
2. `group_key = coalesce(query_id, query)`.
3. Парные суммы считаются по `group_key, sku_group_id`. Знаменатели уровня sku
   (`skg_smooth_atcs_*`, `skg_smooth_orders_*`) остаются на `sku_group_id` и не меняются.
4. Фильтры `query_skg_uniq_impressions_14 >= 2` и `query_skg_uniq_atcs_90 > 0 OR
   query_skg_uniq_orders_90 > 0` применяются на уровне группы, до разворота.
5. Разворот: каждый `query_text` группы получает все пары группы, прошедшие фильтры, включая пары
   с sku, которые с этим конкретным запросом раньше не встречались. Это и есть уплотнение.

Число строк растёт: на замере от 2026-08-09 разворот даёт около 3.3 раза к v2 при среднем размере
группы 1.30, и растёт дальше по мере наполнения справочника. Отсечки топ-N нет, пишутся все пары.

## Колонки сверх зеркала

- `query_id` - ключ группы, по которой посчитана строка. При фолбэке равен самому `query`.
- `has_query_id` - `false` для фолбэка.

Обе колонки служебные, в вектор ranking upload не входят.

## Рантайм

Shared Spark-образ и `git-sync`, шаблон `config/spark/layer_spark_application.yaml`, профиль
ресурсов `large`. Запись - `overwritePartitions()` по `date`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
```

- [ ] **Step 5: Запустить тесты и проверку структуры**

Run:
```bash
python3 ci_test/test_query_id_features.py -v
python3 ci_test/test_script.py
```
Expected: оба PASS.

- [ ] **Step 6: Коммит**

```bash
git add layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid ci_test/test_query_id_features.py
git commit -m "feat(gold): add DAG and README for sku_group_query_atc_order_features_qid"
```

---


### Task 7: Финальная валидация

**Files:**
- Modify: `docs/superpowers/specs/2026-08-10-query-id-features-design.md`

**Interfaces:**
- Consumes: всё, созданное в задачах 1–6.
- Produces: ничего для последующих задач.

- [ ] **Step 1: Прогнать полный набор локальных проверок**

Run:
```bash
python3 ci_test/test_query_id_features.py -v
python3 ci_test/test_script.py
python3 ci_test/test_sync_dbt_sources.py
python3 ci_test/test_sync_iceberg_maintenance.py
python3 scripts/validate_ranking_upload_configs.py
git diff --check
```
Expected: все PASS, `git diff --check` без вывода.

`validate_ranking_upload_configs.py` прогоняется несмотря на то, что `upload/` не менялся:
он заново обходит `layers/**/config.yaml`, и падение здесь означало бы, что новый `config.yaml`
сломал общий парсер.

Если `test_sync_dbt_sources.py` или `test_sync_iceberg_maintenance.py` падают — почти наверняка
они сверяются с полным перечнем таблиц репозитория, и в него нужно добавить две новые. Это
ожидаемое обновление, а не поломка: дописать
`feature_platform_search_query_atc_features_qid` и
`feature_platform_search_sku_group_id_query_atc_order_features_qid` в соответствующий ожидаемый
список внутри теста, ничего больше в нём не меняя.

- [ ] **Step 2: Записать статус реализации в спеку**

В `docs/superpowers/specs/2026-08-10-query-id-features-design.md` заменить строку статуса в шапке

```markdown
Дата: 2026-08-10. Команда: search. Статус: дизайн согласован, план реализации не написан.
```

на

```markdown
Дата: 2026-08-10. Команда: search. Статус: gold-таблицы реализованы, DAG на паузе. Публикация в ranking upload отложена и в этой итерации не делалась.
```

- [ ] **Step 3: Коммит**

```bash
git add ci_test docs/superpowers/specs/2026-08-10-query-id-features-design.md
git commit -m "chore: validate query_id gold entities and record spec status"
```

---

## Отложено: публикация в ranking upload

В этой итерации `upload/features_service_upload/v1/**` **не трогается**. Обе новые gold-таблицы
считаются и живут в Iceberg, но в feature-сервис не уезжают. Соответственно не возникает и
конфликта расписаний: аплоад остаётся в 04:00 и ничего не ждёт от новых DAG.

Когда публикацию решат включить, понадобится отдельный PR со следующим содержимым.

1. Перенести аплоад на `0 7 * * *` UTC. Без этого нельзя: аплоад в 04:00 не может дождаться
   таблицы, которая появляется в 06:00, а `scripts/validate_ranking_upload_configs.py` требует
   `dependency_execution_delta_minutes >= 0`.
2. Пересчитать дельты шести существующих групп как `420 - (минуты крона источника)`:

| Feature group | Крон источника | Сейчас | Станет |
|---|---|---|---|
| `fs_search_query_skg_atc_order_features_v2` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_conversion_features_v2` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_stock_features_v1` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_price_features_v1` | `0 2 * * *` | 120 | 300 |
| `fs_search_query_atc_features_v1` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_rating_v1` | `10 3 * * *` | 50 | 230 |

3. Добавить группы `fs_search_query_atc_features_qid_v1` (source
   `feature_platform_search_query_atc_features_qid`, delta 60) и
   `fs_search_query_skg_atc_order_features_qid_v1` (source
   `feature_platform_search_sku_group_id_query_atc_order_features_qid`, delta 60). Списки `features`
   копируются дословно из `fs_search_query_atc_features_v1` (18 имён) и
   `fs_search_query_skg_atc_order_features_v2` (41 имя), порядок обязан совпадать: он и есть
   контракт сервинга. Служебные `query_id` и `has_query_id` в списки не входят.
4. Вынести обе группы в отдельную модель `search_ranking_qid`, а не дописывать их в
   `search_ranking_main`: тогда они получают собственный TaskGroup со своими сенсорами, и падение
   нового сенсора не блокирует существующий аплоад.
5. Отдельным согласованным шагом — `ranking_service_input.yaml`: дописать два feature-set
   (`QUERY`, size 18 и `SKU_GROUP_TO_QUERY`, size 41) **в самый конец** списка метки `input`.
   Вставка перед существующими элементами сдвинула бы вектор и сломала задеплоенную модель.
   Мержить только когда модель готова потреблять новый сегмент.
6. Согласовать фактический объём с владельцами топика `ranking.features.updates` и
   ranking-сервиса: парная таблица на замере от 2026-08-09 даёт около 3.3 раза к v2, и это число
   вырастет по мере наполнения справочника.

## Выкатка после мержа

Эти шаги выполняются в проде и в план-задачи не входят.

1. Дождаться, пока CI прогонит `run pyspark migrations` на `master` — обе таблицы создадутся.
2. Проверить сгенерированные PR: dbt-trino DQ-источники и Iceberg maintenance в
   `DayMarket/pyspark-etl`.
3. Снять паузу с `feature-platform.layers.gold.query.search_query_atc_features_qid` и
   `feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features_qid`.
4. После первого успешного прогона сравнить объёмы за одну дату:

```sql
SELECT 'v2' AS source, count(*) AS rows_cnt
FROM "dwh-iceberg".gold.feature_platform_search_sku_group_id_query_atc_order_features_v2
WHERE date = DATE '<дата прогона>'
UNION ALL
SELECT 'qid', count(*)
FROM "dwh-iceberg".gold.feature_platform_search_sku_group_id_query_atc_order_features_qid
WHERE date = DATE '<дата прогона>';
```

5. Проверить долю фолбэка:

```sql
SELECT has_query_id, count(*) AS rows_cnt
FROM "dwh-iceberg".gold.feature_platform_search_sku_group_id_query_atc_order_features_qid
WHERE date = DATE '<дата прогона>'
GROUP BY has_query_id;
```

6. Отдельной задачей — бэкфилл `search_query_id` по историческим 90 дням. Без него уплотнение
   работает только на голове трафика: на 2026-08-10 справочник покрывает 7.5 % различных
   `query_text` при 81.5 % показов. После бэкфилла переснять пункты 4–5.
