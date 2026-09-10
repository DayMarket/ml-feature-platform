# Витрина `feature_platform_sku_buyout_features` — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собрать gold-витрину экономики корзины и выкупаемости на грейне `sku_id` и опубликовать её в PostgreSQL `mlgrowth.sku_buyout_features`.

**Architecture:** Trino-source Airflow/Python энтити пишет партиционированную Iceberg-таблицу через `pyiceberg`, читая срезами по диапазону `sku_id`. Отдельный upload-DAG ждёт таску `dq` витрины и заливает последнюю партицию в PostgreSQL через `TRUNCATE` + `COPY` в одной транзакции.

**Tech Stack:** Airflow 3 (`airflow.sdk`), Trino (`TrinoHook`), PyIceberg + PyArrow, `PostgresHook`/psycopg2, pytest/unittest в `ci_test/`.

**Spec:** `docs/superpowers/specs/2026-09-10-sku-buyout-features-design.md`

## Global Constraints

- Airflow-неймспейс — `feature-platform`, не `ml-feature-platform`.
- DAG id витрины: `feature-platform.layers.gold.sku_id.sku_buyout_features`.
- DAG id выгрузки: `feature-platform.upload.buyout_sku_postgres_upload`.
- `table.name` = `feature_platform_sku_buyout_features`, `table.catalog` = `iceberg`, `table.schema` = `gold`, `table.primary_key` = `date,sku_id`.
- Владелец: `table.meta.team: team:buyer`, `dag.team: buyer`, `dag.owner: team:buyer`, `alerts.team: buyer`, `alerts.severity: P2`, `alerts.oncall_webhook_conn_id: team:buyer`.
- `dag.group_tag: buyout-features`, `dag.start_date: "2026-08-20T00:00:00Z"` — общий для всей группы, его проверяет `ci_test/test_buyout_features.py`.
- Расписание обоих DAG'ов: `0 7 * * *` UTC.
- Trino connection витрины: `trino_bx_analytics`. Connection DQ и feature_stats: `trino_search`.
- Runtime image: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.
- PostgreSQL connection: `postgres_non_buyout_service_connect`, schema `mlgrowth`, целевая таблица `sku_buyout_features`.
- `source.shards: 8`, срез задаётся **диапазоном** `sku_id`, не остатком от деления.
- Каждая новая `migrations/create_table.sql` содержит `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')` и комментарий к каждой колонке.
- Все идентификаторы в Trino-SQL квалифицированы полностью: `"dwh-iceberg".gold.…`, `"dwh-clickhouse".dict.…`, `kazanexpress.public.…`.
- PyIceberg-идентификатор — двухэлементный кортеж `(schema, name)`, собранный из `config.yaml`, без `split`/конкатенации.
- Проверка перед завершением: `python3 -m pytest ci_test`, `python3 scripts/validate_dq_configs.py`, `python3 scripts/validate_feature_stats_configs.py`, `python3 scripts/validate_ranking_upload_configs.py`, `python3 scripts/generate_feature_platform_map.py --check`.

---

### Task 1: Миграция и конфиг витрины

**Files:**
- Create: `layers/gold/sku_id/sku_buyout_features/v1/migrations/create_table.sql`
- Create: `layers/gold/sku_id/sku_buyout_features/v1/config.yaml`
- Test: `ci_test/test_sku_buyout_features.py`

**Interfaces:**
- Consumes: ничего.
- Produces: `config.yaml` с ключами `table.{catalog,schema,name,primary_key}`, `source.{engine,trino_conn_id,entity_path,shards}`, `runtime.image`, `dag.{id,group_tag,schedule,start_date,team,owner}`, `alerts.*`, `dq.*`, `feature_stats.*`. Миграция объявляет 22 колонки, перечисленные ниже.

- [ ] **Step 1: Написать падающий тест контракта**

Создать `ci_test/test_sku_buyout_features.py`:

```python
"""Контракт витрины экономики корзины и выкупаемости на грейне sku_id."""

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers" / "gold" / "sku_id" / "sku_buyout_features" / "v1"

# Колонки витрины в порядке миграции. 21 из них уезжают в PostgreSQL;
# category_id остаётся только в Iceberg как ключ джойна с dict.category.
EXPECTED_COLUMNS = (
    "date",
    "sku_id",
    "product_id",
    "seller_id",
    "category_id",
    "l1_category",
    "l2_category",
    "l3_category",
    "l4_category",
    "l5_category",
    "type",
    "commission",
    "cost_price",
    "is_not_block",
    "sku_buyout",
    "product_buyout",
    "category_buyout",
    "shop_buyout",
    "category_no_show",
    "sku_n_delivered",
    "product_n_delivered",
    "predicted_dimensional_group",
)


def read_config() -> dict:
    with (ENTITY / "config.yaml").open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def read_migration() -> str:
    return (ENTITY / "migrations" / "create_table.sql").read_text(encoding="utf-8")


class MigrationContract(unittest.TestCase):
    def test_migration_declares_every_expected_column(self):
        sql = read_migration()
        for column in EXPECTED_COLUMNS:
            with self.subTest(column=column):
                self.assertRegex(sql, rf"\n\s+{column}\s+[A-Z]")

    def test_every_column_carries_a_comment(self):
        sql = read_migration()
        self.assertEqual(
            sql.count("COMMENT '"),
            len(EXPECTED_COLUMNS) + 1,
            "каждая колонка плюс COMMENT самой таблицы",
        )

    def test_migration_disables_hive_lock(self):
        self.assertIn(
            "TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')",
            read_migration(),
        )

    def test_partitioned_by_date(self):
        self.assertIn("PARTITIONED BY (date)", read_migration())


class ConfigContract(unittest.TestCase):
    def test_table_identifier(self):
        config = read_config()
        self.assertEqual(config["table"]["catalog"], "iceberg")
        self.assertEqual(config["table"]["schema"], "gold")
        self.assertEqual(
            config["table"]["name"], "feature_platform_sku_buyout_features"
        )
        self.assertEqual(config["table"]["primary_key"], "date,sku_id")

    def test_shards_are_configured(self):
        self.assertEqual(read_config()["source"]["shards"], 8)

    def test_dq_and_feature_stats_agree_on_partition_template(self):
        config = read_config()
        self.assertEqual(
            config["dq"]["partition_date_template"],
            config["feature_stats"]["partition_date_template"],
        )

    def test_identifier_columns_are_excluded_from_feature_stats(self):
        excluded = set(read_config()["feature_stats"]["exclude_columns"])
        self.assertEqual(
            excluded,
            {
                "product_id",
                "seller_id",
                "category_id",
                "l1_category",
                "l2_category",
                "l3_category",
                "l4_category",
                "l5_category",
            },
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Запустить тест, убедиться что падает**

Run: `python3 -m pytest ci_test/test_sku_buyout_features.py -v`
Expected: FAIL — `FileNotFoundError` на `config.yaml`.

- [ ] **Step 3: Написать миграцию**

`layers/gold/sku_id/sku_buyout_features/v1/migrations/create_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиции, совпадает с датой партиции feature_platform_buyout_online_sku_features',
    sku_id BIGINT COMMENT 'ID товарной позиции (silver.sku.id) — ключ обращения сервиса невыкупов',
    product_id BIGINT COMMENT 'ID карточки товара (silver.sku.product_id); по нему берутся рекламные ставки CPO',
    seller_id BIGINT COMMENT 'ID продавца (silver.sku.seller_id); только для аналитики, на решение модели не влияет',
    category_id BIGINT COMMENT 'ID категории (silver.sku.category_id) — ключ джойна с dict.category; в PostgreSQL не выгружается',
    l1_category BIGINT COMMENT 'Категория 1 уровня из dict.category',
    l2_category BIGINT COMMENT 'Категория 2 уровня; при нуле в справочнике подставляется l1_category',
    l3_category BIGINT COMMENT 'Категория 3 уровня; при нуле подставляется последний ненулевой уровень выше',
    l4_category BIGINT COMMENT 'Категория 4 уровня; при нуле подставляется последний ненулевой уровень выше',
    l5_category BIGINT COMMENT 'Категория 5 уровня; при нуле подставляется последний ненулевой уровень выше',
    type VARCHAR COMMENT 'Тип товара: 1p при продавце is_1p = 1 с известной себестоимостью, иначе 3p',
    commission DECIMAL(5,2) COMMENT 'Процент комиссии из kazanexpress.public.sku.commission; NULL у 1p',
    cost_price BIGINT COMMENT 'Себестоимость за штуку из последней приёмки stock_flow_1p; NULL у 3p',
    is_not_block BOOLEAN COMMENT 'Правило «не отключать постоплату». Источника правила пока нет, колонка всегда false',
    sku_buyout DOUBLE COMMENT 'Выкупаемость sku за 90 дней, стянутая к категории (sku_buyout_rate_shrunk_90d)',
    product_buyout DOUBLE COMMENT 'Выкупаемость карточки товара за 90 дней, стянутая к категории (product_buyout_rate_shrunk_90d)',
    category_buyout DOUBLE COMMENT 'Выкупаемость категории за 90 дней, сглаженная к маркетплейсу (category_buyout_rate_90d); _shrunk-варианта не существует',
    shop_buyout DOUBLE COMMENT 'Выкупаемость магазина за 90 дней, стянутая к маркетплейсу (shop_buyout_rate_shrunk_90d)',
    category_no_show DOUBLE COMMENT 'Доля NO SHOW категории за 90 дней, сглаженная к маркетплейсу (category_no_show_rate_90d)',
    sku_n_delivered BIGINT COMMENT 'Позиций sku доставлено за 90 дней (sku_n_delivered_90d); вес сглаживания',
    product_n_delivered BIGINT COMMENT 'Позиций карточки товара доставлено за 90 дней (product_n_delivered_90d)',
    predicted_dimensional_group VARCHAR COMMENT 'Габаритная группа по сумме height+length+width; при неизвестных габаритах берётся silver.sku.dimensional_group'
)
USING iceberg
COMMENT 'Экономика корзины и выкупаемость на грейне sku_id для сервиса невыкупов: 1p/3p с себестоимостью или комиссией, выкупаемость sku/карточки/категории/магазина в _shrunk-версиях, категорийное дерево l1..l5. Строки — весь sku-универс silver.sku; у sku вне feature_platform_buyout_online_sku_features колонки выкупаемости NULL. Сервис читает последнюю дату: WHERE date = (SELECT max(date) ...)'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
```

- [ ] **Step 4: Написать config.yaml**

`layers/gold/sku_id/sku_buyout_features/v1/config.yaml`:

```yaml
# Trino-source layer pipeline (Airflow/Python + pyiceberg), not Spark.
table:
  key: sku_buyout_features
  catalog: iceberg
  schema: gold
  name: feature_platform_sku_buyout_features
  primary_key: date,sku_id
  meta:
    team: team:buyer

source:
  engine: trino
  trino_conn_id: trino_bx_analytics
  # Имя витрины-источника берётся из её config.yaml, здесь не дублируется.
  entity_path: layers/gold/sku_id/buyout_online_sku_features/v1
  # Партиция читается диапазонами sku_id: 10.5 млн строк не помещаются в память
  # задачи целиком. Диапазон, в отличие от остатка от деления, пушдаунится в
  # kazanexpress.public.sku как index range scan по первичному ключу.
  shards: 8

runtime:
  image: ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2

dag:
  id: feature-platform.layers.gold.sku_id.sku_buyout_features
  group_tag: buyout-features
  schedule: "0 7 * * *"
  start_date: "2026-08-20T00:00:00Z"
  team: buyer
  owner: team:buyer

alerts:
  team: buyer
  severity: P2
  oncall_webhook_conn_id: team:buyer

dq:
  trino_conn_id: trino_search
  partition_date_template: '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  tests:
    # Наблюдённый объём sku-универса на 2026-09-10 — 10 485 803 строки.
    # Порог с запасом вниз на удаление позиций; в warn на время раскатки,
    # перевод в error — после недели реальной истории таблицы.
    - name: row_count_min
      severity: warn
      min_rows: 9000000
    - name: row_count_growth
      severity: warn
    - name: freshness
      severity: warn

feature_stats:
  trino_conn_id: trino_search
  # Дословно совпадает с dq.partition_date_template: обе таски обязаны
  # смотреть на одну партицию, иначе профиль посчитан не по тем данным.
  partition_date_template: '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  # Идентификаторы, а не признаки: профиль по ним бессмысленен.
  exclude_columns:
    - product_id
    - seller_id
    - category_id
    - l1_category
    - l2_category
    - l3_category
    - l4_category
    - l5_category
```

- [ ] **Step 5: Прогнать тест и валидаторы**

Run:
```bash
python3 -m pytest ci_test/test_sku_buyout_features.py -v
python3 scripts/validate_dq_configs.py
python3 scripts/validate_feature_stats_configs.py
```
Expected: тест PASS; оба валидатора завершаются успешно (они рендерят SQL тестов и сверяют каждую колонку с миграцией).

- [ ] **Step 6: Commit**

```bash
git add layers/gold/sku_id/sku_buyout_features/v1/migrations/create_table.sql \
        layers/gold/sku_id/sku_buyout_features/v1/config.yaml \
        ci_test/test_sku_buyout_features.py
git commit -m "feat: add sku_buyout_features table contract and DQ config"
```

---

### Task 2: SQL витрины

**Files:**
- Create: `layers/gold/sku_id/sku_buyout_features/v1/job/__init__.py` (пустой)
- Create: `layers/gold/sku_id/sku_buyout_features/v1/job/query.py`
- Modify: `ci_test/test_sku_buyout_features.py`

**Interfaces:**
- Consumes: имена колонок из миграции Task 1.
- Produces: `build_query(partition_date: datetime.date, signal_table: str, lower: int | None, upper: int | None) -> str`, где `signal_table` — Trino-имя `feature_platform_buyout_online_sku_features`.

- [ ] **Step 1: Написать падающие тесты SQL**

Добавить в `ci_test/test_sku_buyout_features.py`:

```python
import importlib.util
from datetime import date


def load_query_module():
    path = ENTITY / "job" / "query.py"
    spec = importlib.util.spec_from_file_location("sku_buyout_features_query", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


SIGNAL_TABLE = '"dwh-iceberg".gold.feature_platform_buyout_online_sku_features'


class QueryContract(unittest.TestCase):
    def build(self, lower=None, upper=None):
        return load_query_module().build_query(
            date(2026, 9, 9), SIGNAL_TABLE, lower, upper
        )

    def test_pins_the_partition_date(self):
        self.assertIn("DATE '2026-09-09'", self.build())

    def test_never_unions_the_two_type_branches(self):
        # Ветка 3p не фильтрует продавца и содержит все 179 081 sku ветки 1p.
        # UNION задвоил бы их; тип определяется антиджойном.
        sql = self.build().upper()
        self.assertNotIn("UNION", sql)

    def test_type_is_decided_by_the_one_p_antijoin(self):
        sql = self.build()
        self.assertIn("WHEN one_p.sku_id IS NOT NULL THEN '1p'", sql)
        self.assertIn("ELSE '3p'", sql)

    def test_commission_is_null_for_one_p(self):
        self.assertIn("WHEN one_p.sku_id IS NULL THEN comm.commission", self.build())

    def test_uses_shrunk_rates_where_they_exist(self):
        sql = self.build()
        self.assertIn("sku_buyout_rate_shrunk_90d AS sku_buyout", sql)
        self.assertIn("product_buyout_rate_shrunk_90d AS product_buyout", sql)
        self.assertIn("shop_buyout_rate_shrunk_90d AS shop_buyout", sql)

    def test_category_rates_have_no_shrunk_variant(self):
        sql = self.build()
        self.assertIn("category_buyout_rate_90d AS category_buyout", sql)
        self.assertIn("category_no_show_rate_90d AS category_no_show", sql)
        self.assertNotIn("category_buyout_rate_shrunk_90d", sql)

    def test_no_coalesce_cascade_over_buyout_rates(self):
        # _shrunk-колонки уже содержат подстановку родителя, COALESCE был бы
        # вторым сглаживанием поверх первого.
        self.assertNotIn("COALESCE(sku_buyout", self.build())

    def test_category_cascade_starts_at_l1(self):
        # У 23 категорий l2_category = 0; каскад обязан падать до l1.
        sql = self.build()
        self.assertIn("NULLIF(c.l2_category, 0)", sql)
        self.assertIn("NULLIF(c.l5_category, 0)", sql)

    def test_shard_bounds_are_range_predicates_on_every_large_source(self):
        sql = self.build(lower=1000000, upper=2000000)
        self.assertIn("s.id >= 1000000", sql)
        self.assertIn("s.id < 2000000", sql)
        self.assertIn("ke.id >= 1000000", sql)
        self.assertIn("ke.id < 2000000", sql)
        self.assertIn("f.sku_id >= 1000000", sql)

    def test_open_ended_shards_omit_the_missing_bound(self):
        first = self.build(lower=None, upper=2000000)
        self.assertNotIn(">= None", first)
        last = self.build(lower=1000000, upper=None)
        self.assertNotIn("< None", last)

    def test_reads_only_the_requested_signal_partition(self):
        self.assertIn(f"FROM {SIGNAL_TABLE}", self.build())

    def test_is_not_block_is_a_constant(self):
        self.assertIn("false AS is_not_block", self.build())
```

- [ ] **Step 2: Запустить тесты, убедиться что падают**

Run: `python3 -m pytest ci_test/test_sku_buyout_features.py -k Query -v`
Expected: FAIL — `job/query.py` не существует.

- [ ] **Step 3: Написать query.py**

`layers/gold/sku_id/sku_buyout_features/v1/job/query.py`:

```python
"""Trino query для витрины экономики корзины на грейне sku_id.

База строк — весь sku-универс silver.sku. Тип товара определяется антиджойном
к последней приёмке 1p-склада, а не UNION'ом двух веток: ветка 3p не фильтрует
продавца и содержит все sku ветки 1p, поэтому union задвоил бы их.

Категорийное дерево прокидывает вниз последний ненулевой уровень. Справочник
хранит нули вместо NULL, и обрыв всегда сплошной: пар вида «l3 = 0 при l4 <> 0»
в dict.category нет, поэтому пер-уровневый COALESCE эквивалентен каскаду.
"""

from __future__ import annotations

from datetime import date

SKU_TABLE = '"dwh-iceberg".silver.sku'
OLTP_SKU_TABLE = "kazanexpress.public.sku"
STOCK_FLOW_TABLE = '"dwh-clickhouse".marts.stock_flow_1p'
SELLER_TABLE = '"dwh-clickhouse".dict.seller'
CATEGORY_TABLE = '"dwh-clickhouse".dict.category'

ACCEPTANCE_TRANSACTION_TYPE = "EventType.ACCEPTANCE"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _range_predicate(column: str, lower: int | None, upper: int | None) -> str:
    """Предикат среза. Диапазон по sku_id, а не остаток от деления.

    Оба варианта пушдаунятся в Postgres-коннектор, но `id % n = k` не может
    воспользоваться индексом и вызывает полный seq scan OLTP на каждом срезе.
    """
    clauses = []
    if lower is not None:
        clauses.append(f"{column} >= {int(lower)}")
    if upper is not None:
        clauses.append(f"{column} < {int(upper)}")
    return " AND ".join(clauses) if clauses else "TRUE"


def build_query(
    partition_date: date,
    signal_table: str,
    lower: int | None,
    upper: int | None,
) -> str:
    """SQL одного среза партиции; signal_table — Trino-имя gold-источника."""
    partition_date_sql = f"DATE {_sql_string(partition_date.isoformat())}"
    acceptance = _sql_string(ACCEPTANCE_TRANSACTION_TYPE)

    return f"""
WITH one_p AS (
    SELECT sku_id, value_per_piece
    FROM (
        SELECT
            CAST(f.sku_id AS BIGINT) AS sku_id,
            f.value_per_piece,
            ROW_NUMBER() OVER (
                PARTITION BY f.sku_id ORDER BY f.stock_changed_at DESC
            ) AS row_order
        FROM {STOCK_FLOW_TABLE} f
        WHERE f.transaction_type = {acceptance}
          AND {_range_predicate("f.sku_id", lower, upper)}
    ) latest
    WHERE latest.row_order = 1
      AND latest.value_per_piece > 0
      AND latest.sku_id IN (
          SELECT s.id
          FROM {SKU_TABLE} s
          JOIN {SELLER_TABLE} sel ON sel.id = s.seller_id
          WHERE sel.is_1p = 1
            AND {_range_predicate("s.id", lower, upper)}
      )
),

base AS (
    SELECT
        s.id AS sku_id,
        s.product_id,
        s.seller_id,
        s.category_id,
        s.height,
        s."length" AS length_mm,
        s.width,
        s.dimensional_group,
        s.height + s."length" + s.width AS total_size
    FROM {SKU_TABLE} s
    WHERE {_range_predicate("s.id", lower, upper)}
),

dims AS (
    SELECT
        sku_id,
        product_id,
        seller_id,
        category_id,
        CASE
            WHEN total_size < 500 THEN 'SMALL'
            WHEN total_size >= 500 AND total_size < 1700
                 AND height < 500 AND length_mm < 500 AND width < 500 THEN 'MEDIUM'
            WHEN total_size IS NULL THEN dimensional_group
            ELSE 'LARGE'
        END AS predicted_dimensional_group
    FROM base
),

cat AS (
    SELECT
        CAST(c.id AS BIGINT) AS category_id,
        CAST(c.l1_category AS BIGINT) AS l1_category,
        CAST(COALESCE(NULLIF(c.l2_category, 0), c.l1_category) AS BIGINT)
            AS l2_category,
        CAST(COALESCE(
            NULLIF(c.l3_category, 0),
            NULLIF(c.l2_category, 0),
            c.l1_category
        ) AS BIGINT) AS l3_category,
        CAST(COALESCE(
            NULLIF(c.l4_category, 0),
            NULLIF(c.l3_category, 0),
            NULLIF(c.l2_category, 0),
            c.l1_category
        ) AS BIGINT) AS l4_category,
        CAST(COALESCE(
            NULLIF(c.l5_category, 0),
            NULLIF(c.l4_category, 0),
            NULLIF(c.l3_category, 0),
            NULLIF(c.l2_category, 0),
            c.l1_category
        ) AS BIGINT) AS l5_category
    FROM {CATEGORY_TABLE} c
),

comm AS (
    SELECT ke.id AS sku_id, ke.commission
    FROM {OLTP_SKU_TABLE} ke
    WHERE {_range_predicate("ke.id", lower, upper)}
),

feat AS (
    SELECT
        sku_id,
        sku_buyout_rate_shrunk_90d,
        product_buyout_rate_shrunk_90d,
        shop_buyout_rate_shrunk_90d,
        category_buyout_rate_90d,
        category_no_show_rate_90d,
        sku_n_delivered_90d,
        product_n_delivered_90d
    FROM {signal_table}
    WHERE date = {partition_date_sql}
      AND {_range_predicate("sku_id", lower, upper)}
)

SELECT
    {partition_date_sql} AS date,
    dims.sku_id,
    dims.product_id,
    dims.seller_id,
    dims.category_id,
    cat.l1_category,
    cat.l2_category,
    cat.l3_category,
    cat.l4_category,
    cat.l5_category,
    CASE WHEN one_p.sku_id IS NOT NULL THEN '1p' ELSE '3p' END AS type,
    CASE WHEN one_p.sku_id IS NULL THEN comm.commission END AS commission,
    one_p.value_per_piece AS cost_price,
    false AS is_not_block,
    feat.sku_buyout_rate_shrunk_90d AS sku_buyout,
    feat.product_buyout_rate_shrunk_90d AS product_buyout,
    feat.category_buyout_rate_90d AS category_buyout,
    feat.shop_buyout_rate_shrunk_90d AS shop_buyout,
    feat.category_no_show_rate_90d AS category_no_show,
    feat.sku_n_delivered_90d AS sku_n_delivered,
    feat.product_n_delivered_90d AS product_n_delivered,
    dims.predicted_dimensional_group
FROM dims
LEFT JOIN one_p ON one_p.sku_id = dims.sku_id
LEFT JOIN comm ON comm.sku_id = dims.sku_id
LEFT JOIN cat ON cat.category_id = dims.category_id
LEFT JOIN feat ON feat.sku_id = dims.sku_id
"""
```

- [ ] **Step 4: Прогнать тесты**

Run: `python3 -m pytest ci_test/test_sku_buyout_features.py -v`
Expected: PASS.

- [ ] **Step 5: Прогнать SQL на живом Trino**

Отрендерить запрос для одного узкого среза и выполнить через MCP Trino, добавив `LIMIT 100`:

```python
python3 -c "
import importlib.util, datetime
spec = importlib.util.spec_from_file_location('q', 'layers/gold/sku_id/sku_buyout_features/v1/job/query.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
print(m.build_query(datetime.date(2026, 9, 9), '\"dwh-iceberg\".gold.feature_platform_buyout_online_sku_features', 1000000, 1010000))
"
```

Проверить на результате: ровно один ряд на `sku_id`, у строк `type = '1p'` заполнен `cost_price` и пуст `commission`, у `'3p'` — наоборот, `l1..l5_category` не содержат нулей. Диалект Trino локально ничем не исполняется, поэтому этот шаг обязателен (`AGENTS.md`, раздел DQ).

- [ ] **Step 6: Commit**

```bash
git add layers/gold/sku_id/sku_buyout_features/v1/job/ ci_test/test_sku_buyout_features.py
git commit -m "feat: add sku_buyout_features Trino query with 1p/3p antijoin"
```

---

### Task 3: Рантайм витрины и границы срезов

**Files:**
- Create: `layers/gold/sku_id/sku_buyout_features/v1/job/runtime.py`
- Modify: `ci_test/test_sku_buyout_features.py`

**Interfaces:**
- Consumes: `config.yaml` из Task 1.
- Produces: `load_config`, `table_ref`, `parse_interval_timestamp`, `previous_utc_date`, `trino_table_name`, `get_iceberg_catalog`, `preflight_table`, `query_trino`, `require_non_empty`, `write_partition_shard`, `shard_count(config) -> int`, `shard_bounds(min_id, max_id, shards) -> list[tuple[int | None, int | None]]`, `source_id_bounds(conn_id) -> tuple[int, int]`.

- [ ] **Step 1: Написать падающие тесты границ срезов**

Добавить в `ci_test/test_sku_buyout_features.py`:

```python
def load_runtime_module():
    path = ENTITY / "job" / "runtime.py"
    spec = importlib.util.spec_from_file_location("sku_buyout_features_runtime", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ShardBounds(unittest.TestCase):
    def bounds(self, min_id, max_id, shards):
        return load_runtime_module().shard_bounds(min_id, max_id, shards)

    def test_single_shard_has_no_bounds_at_all(self):
        self.assertEqual(self.bounds(8, 12281576, 1), [(None, None)])

    def test_first_shard_has_no_lower_bound(self):
        self.assertIsNone(self.bounds(8, 12281576, 8)[0][0])

    def test_last_shard_has_no_upper_bound(self):
        # sku, заведённые между расчётом границ и чтением, обязаны попасть
        # в последний срез, а не потеряться.
        self.assertIsNone(self.bounds(8, 12281576, 8)[-1][1])

    def test_shards_are_contiguous_without_gaps_or_overlaps(self):
        bounds = self.bounds(8, 12281576, 8)
        for (_, upper), (lower, _) in zip(bounds, bounds[1:]):
            self.assertEqual(upper, lower)

    def test_every_id_in_range_lands_in_exactly_one_shard(self):
        bounds = self.bounds(0, 99, 4)
        for value in (0, 25, 50, 75, 99, -5, 1000):
            matches = [
                1
                for lower, upper in bounds
                if (lower is None or value >= lower)
                and (upper is None or value < upper)
            ]
            with self.subTest(value=value):
                self.assertEqual(sum(matches), 1)

    def test_rejects_non_positive_shard_count(self):
        with self.assertRaises(ValueError):
            self.bounds(0, 99, 0)

    def test_rejects_inverted_range(self):
        with self.assertRaises(ValueError):
            self.bounds(99, 0, 4)


class IdentifierContract(unittest.TestCase):
    def test_table_ref_builds_a_two_part_identifier(self):
        runtime = load_runtime_module()
        ref = runtime.table_ref(read_config())
        self.assertEqual(
            ref.identifier, ("gold", "feature_platform_sku_buyout_features")
        )

    def test_rejects_a_dotted_schema_or_name(self):
        runtime = load_runtime_module()
        for table in (
            {"catalog": "iceberg", "schema": "gold.x", "name": "t"},
            {"catalog": "iceberg", "schema": "gold", "name": "gold.t"},
        ):
            with self.subTest(table=table):
                with self.assertRaises(ValueError):
                    runtime.table_ref({"table": table})


class IntervalParsing(unittest.TestCase):
    def test_accepts_every_airflow_timestamp_shape(self):
        runtime = load_runtime_module()
        for value in (
            "2026-09-10T00:00:00",
            "2026-09-10T00:00:00+00:00",
            "2026-09-10T00:00:00Z",
            "2026-09-10 00:00:00+00:00",
            "2026-09-10 00:00:00",
        ):
            with self.subTest(value=value):
                self.assertEqual(
                    runtime.parse_interval_timestamp(value).date(),
                    date(2026, 9, 10),
                )

    def test_rejects_an_unsupported_value_with_a_clear_message(self):
        runtime = load_runtime_module()
        with self.assertRaises(ValueError) as caught:
            runtime.parse_interval_timestamp("10.09.2026")
        self.assertIn("10.09.2026", str(caught.exception))

    def test_partition_is_the_day_before_the_interval_end(self):
        runtime = load_runtime_module()
        self.assertEqual(
            runtime.previous_utc_date("2026-09-10 07:00:00"), date(2026, 9, 9)
        )
```

- [ ] **Step 2: Запустить тесты, убедиться что падают**

Run: `python3 -m pytest ci_test/test_sku_buyout_features.py -k "ShardBounds or IdentifierContract or IntervalParsing" -v`
Expected: FAIL — `job/runtime.py` не существует.

- [ ] **Step 3: Скопировать рантайм соседа и дописать границы срезов**

```bash
cp layers/gold/sku_id/buyout_online_sku_features/v1/job/runtime.py \
   layers/gold/sku_id/sku_buyout_features/v1/job/runtime.py
```

Заменить docstring первой строки на:

```python
"""Entity-local Airflow/Python runtime для витрины экономики корзины на sku_id."""
```

Удалить `write_daily_snapshot` — витрина пишется срезами, и оставленная функция была бы мёртвым кодом. Вместо неё скопировать дословно `write_partition_shard` из `layers/gold/account_id/buyout_online_account_features/v1/job/runtime.py:205-235`. Затем добавить в конец файла:

```python
def shard_count(config: Mapping[str, Any]) -> int:
    source = config.get("source")
    if not isinstance(source, Mapping):
        raise ValueError("config.yaml must contain a source mapping")
    raw = source.get("shards")
    try:
        shards = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"config source.shards must be an integer, got {raw!r}") from exc
    if shards <= 0:
        raise ValueError(f"config source.shards must be positive, got {shards}")
    return shards


def shard_bounds(
    min_id: int, max_id: int, shards: int
) -> list[tuple[int | None, int | None]]:
    """Полуинтервалы [lower, upper) по sku_id, покрывающие всю числовую ось.

    У первого среза нет нижней границы, у последнего — верхней: sku, заведённый
    между расчётом границ и чтением источника, обязан попасть в последний срез,
    а не потеряться между границами.
    """
    if shards < 1:
        raise ValueError(f"shards must be positive, got {shards}")
    if max_id < min_id:
        raise ValueError(f"max_id {max_id} is below min_id {min_id}")

    span = max_id - min_id + 1
    width = -(-span // shards)
    bounds: list[tuple[int | None, int | None]] = []
    for shard in range(shards):
        lower = None if shard == 0 else min_id + shard * width
        upper = None if shard == shards - 1 else min_id + (shard + 1) * width
        bounds.append((lower, upper))
    return bounds


def source_id_bounds(conn_id: str) -> tuple[int, int]:
    """min/max sku_id по silver.sku — дешёвый запрос к Iceberg перед срезами."""
    frame = query_trino(
        conn_id,
        f"SELECT min(id) AS min_id, max(id) AS max_id FROM {SKU_TABLE}",
    )
    if frame.empty or frame.loc[0, "min_id"] is None:
        raise RuntimeError(f"{SKU_TABLE} returned no id bounds; refusing to shard")
    return int(frame.loc[0, "min_id"]), int(frame.loc[0, "max_id"])
```

Добавить рядом с прочими константами:

```python
SKU_TABLE = '"dwh-iceberg".silver.sku'
```

- [ ] **Step 4: Прогнать тесты**

Run: `python3 -m pytest ci_test/test_sku_buyout_features.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add layers/gold/sku_id/sku_buyout_features/v1/job/runtime.py ci_test/test_sku_buyout_features.py
git commit -m "feat: add sku_buyout_features runtime with range shard bounds"
```

---

### Task 4: DAG витрины, README и реестр группы

**Files:**
- Create: `layers/gold/sku_id/sku_buyout_features/v1/dag.py`
- Create: `layers/gold/sku_id/sku_buyout_features/v1/README.md`
- Modify: `ci_test/test_buyout_features.py:28-31` (START_DATE и реестр `ENTITIES`)
- Modify: `ci_test/test_buyout_features.py:394-416` (`test_schedule_and_start_date`)

**Interfaces:**
- Consumes: `build_query` (Task 2), `shard_count`/`shard_bounds`/`source_id_bounds`/`write_partition_shard` (Task 3).
- Produces: DAG с тасками `wait_for_buyout_online_sku_dq`, `materialize`, `dq`, `feature_stats`.

- [ ] **Step 1: Добавить энтити в реестр группы**

В `ci_test/test_buyout_features.py` добавить в `ENTITIES` после записи `online_sku`:

```python
    "sku_buyout": {
        "layer": "gold",
        "group": "sku_id",
        "entity": "sku_buyout_features",
        "table": "iceberg.gold.feature_platform_sku_buyout_features",
        "primary_key": ("date", "sku_id"),
        "schedule": "0 7 * * *",
        "engine": "trino",
        "dq_sources": (
            (
                "online_sku",
                "feature-platform.layers.gold.sku_id."
                "buyout_online_sku_features",
            ),
        ),
    },
```

- [ ] **Step 2: Запустить реестровые тесты, убедиться что падают**

Run: `python3 -m pytest ci_test/test_buyout_features.py -v`
Expected: FAIL — `test_table_identifiers_match_registry` и соседние тесты не находят `dag.py`; `test_schedule_and_start_date` не находит расписание.

- [ ] **Step 3: Написать dag.py**

`layers/gold/sku_id/sku_buyout_features/v1/dag.py` — по образцу `layers/gold/sku_id/buyout_online_sku_features/v1/dag.py`, с двумя отличиями: цикл по срезам и запрос границ перед ним.

```python
"""Собрать экономику корзины и выкупаемость на грейне sku_id после DQ источника."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
JOB_DIR = os.path.join(ENTITY_DIR, "job")


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)

SOURCE_CONFIG_PATH = os.path.join(REPO_ROOT, CONFIG["source"]["entity_path"], "config.yaml")
SOURCE_DAG_ID = "feature-platform.layers.gold.sku_id.buyout_online_sku_features"
SOURCE_DQ_TASK_ID = "dq"
# Источник пишет партицию в 06:00 UTC, витрина стартует в 07:00 UTC:
# D 07:00 - 1ч = D 06:00 — логическая дата прогона источника за ту же партицию.
SOURCE_DQ_EXECUTION_DELTA = timedelta(hours=1)


def _load_module(filename: str, module_name: str):
    path = os.path.join(JOB_DIR, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _executor_config() -> dict:
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=CONFIG["runtime"]["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "16Gi", "cpu": "4"},
                            limits={"memory": "16Gi"},
                        ),
                    )
                ]
            )
        )
    }


def get_dag_default_args() -> dict:
    return {
        "owner": CONFIG["dag"]["owner"],
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "max_retry_delay": timedelta(minutes=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"],
            oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"],
        ),
    }


@dag(
    default_args=get_dag_default_args(),
    dag_id=CONFIG["dag"]["id"],
    max_active_runs=1,
    tags=[
        "feature-platform",
        CONFIG["dag"]["group_tag"],
        CONFIG["dag"]["team"],
        "gold",
        "buyout",
        "sku",
    ],
    dagrun_timeout=timedelta(hours=4),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def sku_buyout_features_dag() -> None:
    wait_for_source_dq = ExternalTaskSensor(
        task_id="wait_for_buyout_online_sku_dq",
        external_dag_id=SOURCE_DAG_ID,
        external_task_id=SOURCE_DQ_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        execution_delta=SOURCE_DQ_EXECUTION_DELTA,
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )

    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "sku_buyout_features_runtime")
        query = _load_module("query.py", "sku_buyout_features_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        source_ref = runtime.table_ref(runtime.load_config(SOURCE_CONFIG_PATH))
        if source_ref.catalog != ref.catalog:
            raise ValueError(
                "Source and output configs must use one Iceberg catalog; "
                f"output={ref.catalog!r}, source={source_ref.catalog!r}"
            )

        catalog = runtime.get_iceberg_catalog(ref)
        # Resolve both migrated tables before running the expensive source query.
        table = runtime.preflight_table(catalog, ref)
        runtime.preflight_table(catalog, source_ref)

        partition_date = runtime.previous_utc_date(interval_end_value)
        source_table = runtime.trino_table_name(source_ref)
        conn_id = config["source"]["trino_conn_id"]

        min_id, max_id = runtime.source_id_bounds(conn_id)
        bounds = runtime.shard_bounds(min_id, max_id, runtime.shard_count(config))

        for index, (lower, upper) in enumerate(bounds):
            sql = query.build_query(partition_date, source_table, lower, upper)
            frame = runtime.query_trino(conn_id, sql)
            if index == 0:
                runtime.require_non_empty(frame, partition_date)
            runtime.write_partition_shard(
                table,
                frame,
                partition_date,
                replace=index == 0,
            )

    gold_task = materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )
    wait_for_source_dq >> gold_task

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: downstream ждёт
    # таску dq, поэтому падение профилей не блокирует потребителей.
    gold_task >> [dq_task, stats_task]


dag = sku_buyout_features_dag()
```

- [ ] **Step 4: Написать README**

`layers/gold/sku_id/sku_buyout_features/v1/README.md` — русскоязычный, обязан содержать: полное имя таблицы `iceberg.gold.feature_platform_sku_buyout_features`, точный DAG id, `group_tag: buyout-features`, список источников с ролями, правило антиджойна 1p/3p и его численное обоснование (179 081 sku пересечения, 34 979 sku 1p-продавцов без себестоимости уезжают в `'3p'`, 25 256 приёмок 1p при 3p-продавце отсекаются), правило каскада категорий, объяснение почему `_shrunk` и почему `COALESCE` не нужен, скоуп строк и NULL-семантику у ~7.6 млн sku вне витрины выкупаемости, диапазонное шардирование и его причину, контракт чтения потребителем (`WHERE date = (SELECT max(date) ...)`), раздел про выгрузку в `mlgrowth.sku_buyout_features` и про отсутствие `category_id` в целевой таблице.

- [ ] **Step 5: Прогнать реестровые тесты**

Run: `python3 -m pytest ci_test/test_buyout_features.py ci_test/test_sku_buyout_features.py -v`
Expected: PASS. Если `test_schedule_and_start_date` требует общий `START_DATE`, а расписание новой энтити отличается — тест уже читает `schedule` из реестра, поэтому правка не нужна; при падении на `start_date` добавить в реестр ключ `"start_date"` с дефолтом `START_DATE` и читать его в тесте.

- [ ] **Step 6: Commit**

```bash
git add layers/gold/sku_id/sku_buyout_features/v1/dag.py \
        layers/gold/sku_id/sku_buyout_features/v1/README.md \
        ci_test/test_buyout_features.py
git commit -m "feat: add sku_buyout_features DAG with sharded materialize"
```

---

### Task 5: Дискриминатор `sink.type` в валидаторе upload-конфигов

**Files:**
- Modify: `scripts/validate_ranking_upload_configs.py:389-420` (`main`)
- Modify: `ci_test/test_validate_ranking_upload_configs.py`

**Interfaces:**
- Consumes: ничего.
- Produces: `sink_type(config: dict) -> str` — возвращает `config["sink"]["type"]` или `"kafka"` при отсутствии ключа. `main` пропускает ranking-специфичные проверки при `sink_type != "kafka"`.

- [ ] **Step 1: Написать падающий тест**

Добавить в `ci_test/test_validate_ranking_upload_configs.py`:

```python
def check_postgres_sink_skips_ranking_checks(validator) -> list[str]:
    """Postgres-выгрузка не имеет models и не обязана иметь ranking-группы."""
    errors = []
    if validator.sink_type({}) != "kafka":
        errors.append("конфиг без ключа sink обязан считаться kafka-выгрузкой")
    if validator.sink_type({"sink": {"type": "postgres"}}) != "postgres":
        errors.append("sink.type postgres не распознан")
    postgres_config = {
        "sink": {"type": "postgres"},
        "feature_groups": [
            {
                "name": "sku_buyout_features_postgres",
                "source": {
                    "schema": "gold",
                    "table": "feature_platform_sku_buyout_features",
                    "dependency_dag_id": (
                        "feature-platform.layers.gold.sku_id.sku_buyout_features"
                    ),
                    "dependency_execution_delta_minutes": 0,
                    "dependency_task_id": "dq",
                },
                "features": ["sku_buyout"],
            }
        ],
    }
    model_errors = validator.validate_models(
        Path("upload/buyout_sku_postgres_upload/v1/config.yaml"),
        postgres_config,
        {},
    )
    if model_errors:
        errors.append(
            "validate_models не должен вызываться для postgres-выгрузки, "
            f"получено: {model_errors}"
        )
    return errors
```

И вызвать её из `main()` этого теста рядом с существующими проверками.

- [ ] **Step 2: Запустить тест, убедиться что падает**

Run: `python3 ci_test/test_validate_ranking_upload_configs.py`
Expected: FAIL — `AttributeError: module has no attribute 'sink_type'`.

- [ ] **Step 3: Добавить `sink_type` и ветвление в `main`**

В `scripts/validate_ranking_upload_configs.py` перед `def main()` добавить:

```python
KAFKA_SINK = "kafka"


def sink_type(config: dict[str, Any]) -> str:
    """Тип потребителя выгрузки. Отсутствие ключа — исторический Kafka-контракт."""
    sink = config.get("sink")
    if not isinstance(sink, dict):
        return KAFKA_SINK
    value = sink.get("type")
    return str(value).strip() if isinstance(value, str) and value.strip() else KAFKA_SINK
```

В `main()` заменить блок обработки одного конфига на:

```python
    for config_path in config_paths:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config_sink = sink_type(config)
        feature_groups = config.get("feature_groups", [])
        if not feature_groups:
            errors.append(f"{config_path}: feature_groups must not be empty")
        feature_groups_by_name = {
            str(feature_group.get("name", "")): feature_group
            for feature_group in feature_groups
        }
        # models и глобальная уникальность имён групп — контракт protobuf-вектора
        # ranking-сервиса. У выгрузки в PostgreSQL их нет: колонки адресуются
        # по имени, а не по позиции.
        if config_sink == KAFKA_SINK:
            errors.extend(validate_models(config_path, config, feature_groups_by_name))
        for feature_group in feature_groups:
            group_name = feature_group.get("name", "")
            if config_sink == KAFKA_SINK:
                if group_name in group_names:
                    errors.append(
                        f"{config_path}: duplicate ranking feature group name {group_name}"
                    )
                group_names.add(group_name)
            # Сверка колонок с миграциями источника полезна любой выгрузке.
            errors.extend(validate_feature_group(config_path, feature_group, tables))
        if not errors and config_sink == KAFKA_SINK:
            print_model_components(config_path, config)
```

- [ ] **Step 4: Прогнать тесты**

Run:
```bash
python3 ci_test/test_validate_ranking_upload_configs.py
python3 scripts/validate_ranking_upload_configs.py
```
Expected: оба завершаются успешно; существующие Kafka-конфиги валидируются как раньше.

- [ ] **Step 5: Commit**

```bash
git add scripts/validate_ranking_upload_configs.py ci_test/test_validate_ranking_upload_configs.py
git commit -m "feat: let upload configs declare a non-kafka sink"
```

---

### Task 6: Выгрузка в PostgreSQL

**Files:**
- Create: `upload/buyout_sku_postgres_upload/v1/config.yaml`
- Create: `upload/buyout_sku_postgres_upload/v1/job/__init__.py` (пустой)
- Create: `upload/buyout_sku_postgres_upload/v1/job/upload_postgres.py`
- Create: `upload/buyout_sku_postgres_upload/v1/dag.py`
- Create: `upload/buyout_sku_postgres_upload/v1/README.md`
- Test: `ci_test/test_buyout_sku_postgres_upload.py`

**Interfaces:**
- Consumes: `sink_type` (Task 5), витрина Task 1–4, `table_ref`/`get_iceberg_catalog`/`preflight_table` из `layers/gold/sku_id/sku_buyout_features/v1/job/runtime.py` (Task 3).
- Produces: `batch_to_csv(batch, columns: Sequence[str], updated_at: datetime) -> io.StringIO`; `copy_partition(cursor, batches: Iterable, columns: Sequence[str], target_table: str, updated_at: datetime) -> int`; `publish(iceberg_table, partition_date: date, columns: Sequence[str], connection, target_table: str, updated_at: datetime) -> int`.

- [ ] **Step 1: Написать падающие тесты**

Создать `ci_test/test_buyout_sku_postgres_upload.py`:

```python
"""Контракт выгрузки витрины sku_buyout_features в PostgreSQL."""

import importlib.util
import io
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPLOAD = ROOT / "upload" / "buyout_sku_postgres_upload" / "v1"

# Порядок и состав колонок целевой таблицы mlgrowth.sku_buyout_features.
# category_id в неё не выгружается.
TARGET_COLUMNS = (
    "sku_id",
    "product_id",
    "seller_id",
    "l1_category",
    "l2_category",
    "l3_category",
    "l4_category",
    "l5_category",
    "type",
    "commission",
    "cost_price",
    "predicted_dimensional_group",
    "is_not_block",
    "sku_buyout",
    "product_buyout",
    "category_buyout",
    "shop_buyout",
    "category_no_show",
    "sku_n_delivered",
    "product_n_delivered",
)


def read_config() -> dict:
    return json.loads((UPLOAD / "config.yaml").read_text(encoding="utf-8"))


def load_job():
    path = UPLOAD / "job" / "upload_postgres.py"
    spec = importlib.util.spec_from_file_location("upload_postgres", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ConfigContract(unittest.TestCase):
    def test_declares_a_postgres_sink(self):
        config = read_config()
        self.assertEqual(config["sink"]["type"], "postgres")
        self.assertEqual(
            config["sink"]["connection_id"], "postgres_non_buyout_service_connect"
        )
        self.assertEqual(config["sink"]["schema"], "mlgrowth")
        self.assertEqual(config["sink"]["table"], "sku_buyout_features")

    def test_waits_for_the_gold_dq_task(self):
        source = read_config()["feature_groups"][0]["source"]
        self.assertEqual(source["dependency_task_id"], "dq")
        self.assertEqual(
            source["dependency_dag_id"],
            "feature-platform.layers.gold.sku_id.sku_buyout_features",
        )
        self.assertEqual(source["dependency_execution_delta_minutes"], 0)

    def test_does_not_publish_category_id(self):
        features = read_config()["feature_groups"][0]["features"]
        self.assertNotIn("category_id", features)
        self.assertEqual(tuple(features), TARGET_COLUMNS)

    def test_has_no_source_limit(self):
        self.assertNotIn("limit", read_config()["feature_groups"][0]["source"])


class CsvSerialisation(unittest.TestCase):
    def test_nulls_stay_null_and_are_not_zero_filled(self):
        # Пропуск в выкупаемости — законное значение, нулём его заменять нельзя.
        import pyarrow as pa

        job = load_job()
        batch = pa.RecordBatch.from_pydict(
            {"sku_id": [1], "sku_buyout": [None]},
        )
        stamp = datetime(2026, 9, 10, 7, 0, tzinfo=timezone.utc)
        buffer = job.batch_to_csv(batch, ("sku_id", "sku_buyout"), stamp)
        row = buffer.getvalue().strip()
        self.assertEqual(row, "1,,2026-09-10 07:00:00+00:00")

    def test_appends_the_run_timestamp_as_last_column(self):
        import pyarrow as pa

        job = load_job()
        batch = pa.RecordBatch.from_pydict({"sku_id": [7]})
        stamp = datetime(2026, 9, 10, 7, 0, tzinfo=timezone.utc)
        buffer = job.batch_to_csv(batch, ("sku_id",), stamp)
        self.assertTrue(buffer.getvalue().strip().endswith("2026-09-10 07:00:00+00:00"))


class TransactionContract(unittest.TestCase):
    def test_truncate_and_copy_share_one_transaction(self):
        source = (UPLOAD / "job" / "upload_postgres.py").read_text(encoding="utf-8")
        self.assertIn("TRUNCATE TABLE", source)
        self.assertIn("conn.commit()", source)
        self.assertIn("conn.rollback()", source)
        self.assertNotIn("autocommit = True", source)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Запустить тесты, убедиться что падают**

Run: `python3 -m pytest ci_test/test_buyout_sku_postgres_upload.py -v`
Expected: FAIL — `config.yaml` не существует.

- [ ] **Step 3: Написать config.yaml**

`upload/buyout_sku_postgres_upload/v1/config.yaml` (upload-конфиги в этом репозитории — JSON):

```json
{
  "sink": {
    "type": "postgres",
    "connection_id": "postgres_non_buyout_service_connect",
    "schema": "mlgrowth",
    "table": "sku_buyout_features"
  },
  "runtime": {
    "image": "ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2"
  },
  "dag": {
    "id": "feature-platform.upload.buyout_sku_postgres_upload",
    "schedule": "0 7 * * *",
    "start_date": "2026-09-10T00:00:00+00:00",
    "team": "buyer",
    "team_tag": "team::buyer",
    "owner": "team:buyer"
  },
  "alerts": {
    "team": "buyer",
    "severity": "P3",
    "oncall_webhook_conn_id": "team:buyer"
  },
  "feature_groups": [
    {
      "source": {
        "schema": "gold",
        "table": "feature_platform_sku_buyout_features",
        "dependency_dag_id": "feature-platform.layers.gold.sku_id.sku_buyout_features",
        "dependency_execution_delta_minutes": 0,
        "dependency_task_id": "dq"
      },
      "name": "sku_buyout_features_postgres",
      "features": [
        "sku_id",
        "product_id",
        "seller_id",
        "l1_category",
        "l2_category",
        "l3_category",
        "l4_category",
        "l5_category",
        "type",
        "commission",
        "cost_price",
        "predicted_dimensional_group",
        "is_not_block",
        "sku_buyout",
        "product_buyout",
        "category_buyout",
        "shop_buyout",
        "category_no_show",
        "sku_n_delivered",
        "product_n_delivered"
      ]
    }
  ]
}
```

- [ ] **Step 4: Написать job/upload_postgres.py**

```python
"""Публикация партиции витрины sku_buyout_features в PostgreSQL сервиса невыкупов."""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from typing import Iterable, Sequence

logger = logging.getLogger("airflow.task")


def batch_to_csv(batch, columns: Sequence[str], updated_at: datetime) -> io.StringIO:
    """CSV одного батча. Пустое поле — NULL: пропуск в признаке нулём не заменяется."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    table = batch.to_pydict()
    row_count = len(table[columns[0]]) if columns else 0
    for index in range(row_count):
        row = []
        for column in columns:
            value = table[column][index]
            row.append("" if value is None else value)
        row.append(updated_at.isoformat(sep=" "))
        writer.writerow(row)
    buffer.seek(0)
    return buffer


def copy_partition(
    cursor,
    batches: Iterable,
    columns: Sequence[str],
    target_table: str,
    updated_at: datetime,
) -> int:
    """TRUNCATE плюс COPY всех батчей. Вызывается внутри открытой транзакции."""
    cursor.execute(f"TRUNCATE TABLE {target_table}")
    column_list = ", ".join(list(columns) + ["updated_at"])
    statement = f"COPY {target_table} ({column_list}) FROM STDIN WITH (FORMAT csv)"
    written = 0
    for batch in batches:
        buffer = batch_to_csv(batch, columns, updated_at)
        cursor.copy_expert(statement, buffer)
        written += batch.num_rows
    return written


def publish(
    iceberg_table,
    partition_date: date,
    columns: Sequence[str],
    connection,
    target_table: str,
    updated_at: datetime,
) -> int:
    """Заменить содержимое целевой таблицы партицией витрины за одну транзакцию."""
    from pyiceberg.expressions import EqualTo

    scan = iceberg_table.scan(
        row_filter=EqualTo("date", partition_date),
        selected_fields=tuple(columns),
    )
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            written = copy_partition(
                cursor,
                scan.to_arrow_batch_reader(),
                columns,
                target_table,
                updated_at,
            )
            if written == 0:
                # Пустая партиция затёрла бы рабочую таблицу сервиса.
                raise RuntimeError(
                    f"Partition {partition_date} of {iceberg_table.name()} is empty; "
                    f"refusing to leave {target_table} truncated"
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    logger.info("Copied %d rows into %s", written, target_table)
    return written
```

- [ ] **Step 5: Написать dag.py**

`upload/buyout_sku_postgres_upload/v1/dag.py`:

```python
"""Опубликовать последнюю партицию sku_buyout_features в PostgreSQL сервиса невыкупов."""

import importlib.util
import json
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

UPLOAD_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(UPLOAD_DIR, "..", ".."))

CONFIG_PATH = os.path.join(UPLOAD_DIR, "config.yaml")
with open(CONFIG_PATH, encoding="utf-8") as config_stream:
    CONFIG = json.load(config_stream)

FEATURE_GROUP = CONFIG["feature_groups"][0]
SOURCE = FEATURE_GROUP["source"]
SINK = CONFIG["sink"]

SOURCE_ENTITY_DIR = os.path.join(
    REPO_ROOT, "layers", "gold", "sku_id", "sku_buyout_features", "v1"
)
SOURCE_CONFIG_PATH = os.path.join(SOURCE_ENTITY_DIR, "config.yaml")


def _load_module(path: str, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _executor_config() -> dict:
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=CONFIG["runtime"]["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "8Gi", "cpu": "2"},
                            limits={"memory": "8Gi"},
                        ),
                    )
                ]
            )
        )
    }


default_args = {
    "owner": CONFIG["dag"]["owner"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_oncall_notification(
        team=CONFIG["alerts"]["team"],
        oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
        severity=CONFIG["alerts"]["severity"],
    ),
}


@dag(
    default_args=default_args,
    dag_id=CONFIG["dag"]["id"],
    max_active_runs=1,
    tags=[
        "feature-platform",
        "buyout-features",
        CONFIG["dag"]["team"],
        "upload",
        "postgres",
    ],
    dagrun_timeout=timedelta(hours=4),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(cron=CONFIG["dag"]["schedule"], timezone="UTC"),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def buyout_sku_postgres_upload() -> None:
    wait_for_gold_dq = ExternalTaskSensor(
        task_id="wait_for_sku_buyout_features_dq",
        external_dag_id=SOURCE["dependency_dag_id"],
        external_task_id=SOURCE["dependency_task_id"],
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        execution_delta=timedelta(
            minutes=SOURCE["dependency_execution_delta_minutes"]
        ),
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )

    @task(executor_config=_executor_config())
    def publish_to_postgres(interval_end_value: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        runtime = _load_module(
            os.path.join(SOURCE_ENTITY_DIR, "job", "runtime.py"),
            "sku_buyout_features_runtime",
        )
        job = _load_module(
            os.path.join(UPLOAD_DIR, "job", "upload_postgres.py"),
            "buyout_sku_upload_postgres",
        )

        ref = runtime.table_ref(runtime.load_config(SOURCE_CONFIG_PATH))
        catalog = runtime.get_iceberg_catalog(ref)
        iceberg_table = runtime.preflight_table(catalog, ref)

        # Та же партиция, что записала витрина: дата конца интервала минус сутки.
        partition_date = runtime.previous_utc_date(interval_end_value)
        target_table = f'{SINK["schema"]}.{SINK["table"]}'
        connection = PostgresHook(
            postgres_conn_id=SINK["connection_id"],
            schema=SINK["schema"],
        ).get_conn()

        job.publish(
            iceberg_table,
            partition_date,
            tuple(FEATURE_GROUP["features"]),
            connection,
            target_table,
            pendulum.now("UTC"),
        )

    wait_for_gold_dq >> publish_to_postgres(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )


dag = buyout_sku_postgres_upload()
```

- [ ] **Step 6: Написать README**

`upload/buyout_sku_postgres_upload/v1/README.md`: точный DAG id, целевая таблица `mlgrowth.sku_buyout_features`, connection, режим `TRUNCATE` + `COPY` в одной транзакции и что видит читатель во время выгрузки, отказ публиковать пустую партицию, отсутствие `category_id` в целевой таблице, приведение `sku_n_delivered`/`product_n_delivered` к `integer` и замеренный запас (максимум 234 462), тот факт что DDL целевой таблицы живёт вне репозитория и список колонок надо держать синхронным вручную.

- [ ] **Step 7: Проверить, что рантайм-зависимости есть в образе**

Две зависимости этой таски не используются больше нигде в репозитории и их наличие в `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2` не подтверждено ни одним существующим DAG'ом:

```bash
docker run --rm ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2 python -c \
  "from airflow.providers.postgres.hooks.postgres import PostgresHook; \
   import pyiceberg, pyiceberg.table; \
   assert hasattr(pyiceberg.table.DataScan, 'to_arrow_batch_reader'); \
   print('ok', pyiceberg.__version__)"
```

Expected: `ok <version>`. Если провайдер Postgres отсутствует — остановиться и согласовать с пользователем кастомный образ по разделу «Custom Image Workflow» в `AGENTS.md`; собирать его молча нельзя. Если отсутствует `to_arrow_batch_reader` — заменить в `publish` на `scan.to_arrow()` с однократной сериализацией и поднять память таски до 32Gi, зафиксировав причину в README.

- [ ] **Step 8: Прогнать тесты и валидатор**

Run:
```bash
python3 -m pytest ci_test/test_buyout_sku_postgres_upload.py -v
python3 scripts/validate_ranking_upload_configs.py
```
Expected: тесты PASS; валидатор не падает на новом конфиге и по-прежнему сверяет 20 колонок с миграцией витрины.

- [ ] **Step 9: Commit**

```bash
git add upload/buyout_sku_postgres_upload/ ci_test/test_buyout_sku_postgres_upload.py
git commit -m "feat: publish sku_buyout_features to mlgrowth PostgreSQL"
```

---

### Task 7: Карта зависимостей и полная валидация

**Files:**
- Modify: `docs/feature_platform_map.md` (перегенерировать, не править руками)

**Interfaces:**
- Consumes: всё, созданное в Task 1–6.
- Produces: ничего.

- [ ] **Step 1: Перегенерировать карту**

Run: `python3 scripts/generate_feature_platform_map.py`

- [ ] **Step 2: Проверить, что карта сошлась с кодом**

Run: `python3 scripts/generate_feature_platform_map.py --check`
Expected: успех. Проверить глазами, что в карте появились оба новых DAG'а и ребро `sku_buyout_features → buyout_sku_postgres_upload`, и что ни один из них не попал в раздел «Что генератор не смог прочитать».

- [ ] **Step 3: Прогнать весь набор проверок**

Run:
```bash
python3 -m pytest ci_test
python3 scripts/validate_dq_configs.py
python3 scripts/validate_feature_stats_configs.py
python3 scripts/validate_ranking_upload_configs.py
git diff --check
```
Expected: всё зелёное, кроме предсуществующих падений — зафиксировать их список до начала работы и сверить, что он не вырос.

- [ ] **Step 4: Commit**

```bash
git add docs/feature_platform_map.md
git commit -m "chore: regenerate feature platform map for sku_buyout_features"
```

---

## Post-merge follow-up

- Проверить PR, которые master-side CI заведёт в `DayMarket/dbt-trino` (dbt source/DQ) и в `DayMarket/pyspark-etl` (Iceberg maintenance) для новой таблицы.
- Решить судьбу производителя `mlgrowth.public.sku_commission_info`: новая витрина покрывает его колонки, и два процесса не должны писать пересекающиеся данные.
- После недели истории таблицы поднять `row_count_min`, `row_count_growth` и `freshness` из `warn` в `error`, подобрав пороги по наблюдённому диапазону.
