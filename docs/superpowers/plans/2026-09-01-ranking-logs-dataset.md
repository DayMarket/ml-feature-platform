# Ranking Logs Dataset v1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собрать недельный датасет «запрос × кандидат» из `silver.ranking_analytics_events` для подбора параметров ранжирующей формулы поиска.

**Architecture:** Новая энтити `datasets/search/ranking_logs/v1/` по образцу `datasets/search/search_ranking/v1/`: SparkApplication читает 7 суточных партиций источника, детерминированно отбирает запросы по хэшу `request_id`, разворачивает выровненные массивы через `posexplode(arrays_zip(...))`, обогащает возрастом sku-группы, рейтингом и частотностью запроса и пишет одну партицию `collection_date` в Iceberg. DAG: `collect >> [dq, feature_stats]`.

**Tech Stack:** PySpark 3.5.5 (Spark SQL), Iceberg 1.5.2, Airflow 3 (`airflow.sdk`), SparkKubernetesOperator, внутренние пакеты `dq/` и `feature_stats/`, pytest.

**Spec:** `docs/superpowers/specs/2026-09-01-ranking-logs-dataset-design.md`

## Global Constraints

- Путь энтити: `datasets/search/ranking_logs/v1/` — ровно этот, без промежуточных каталогов.
- Таблица: `iceberg.silver.feature_platform_ranking_logs_dataset_v1`.
- DAG id: `feature-platform.datasets.search.ranking_logs.v1`.
- Расписание: `0 12 * * 0` UTC, `start_date` — timezone-aware UTC.
- Партиция: `collection_date`, шаблон партиции **в `dq` и `feature_stats` дословно одинаковый**: `'{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'`.
- Первичный ключ: `collection_date,event_date,request_id,sku_group_id`.
- Владелец: `table.meta.team: team:search`, `dag.team: search`, `alerts.team: search`, `severity: P4`, `oncall_webhook_conn_id: oncall_webhook_search`.
- Все DQ-тесты в severity `warn` (политика репозитория для событийных датасетов).
- Датасет не выгружается ни в ranking-service, ни в любой другой онлайн-контур.
- Каталог в Spark SQL — `iceberg`, не `dwh-iceberg` (последнее — имя каталога только в Trino).
- Ни одна колонка `model_input['input']` (145 признаков) в датасет не попадает.
- Все команды запускаются из корня репозитория. **Не использовать `cd`**: путь содержит неразрывный пробел, и `cd` уводит в соседний каталог.

## Отклонение от спеки, требующее решения

Спека (раздел 6) закладывала DQ-тест `row_count_growth`. Он для недельной энтити не работает: `dq/tests.py:66-72` жёстко берёт baseline как `partition_date - 1 day`, а при `partition_granularity: date` это не настраивается. Для недельных партиций предыдущий день всегда пуст, тест вернёт `failed_rows = -1` («нет baseline») на каждом ране и не проверит ничего.

Решение в этом плане: `row_count_growth` явно отключается через `enabled: false` в config.yaml. Явное отключение необходимо, потому что `dq/config.py` инъецирует базовые тесты в разрешённые параметры: любой базовый тест, отсутствующий из явного списка, получает severity `error` по умолчанию. Причина отключения фиксируется комментарием в `config.yaml` и строкой в README. Поддержка недельного baseline в `dq/` — отдельная работа вне этого плана.

---

### Task 1: Конфиг, миграция и README энтити

**Files:**
- Create: `datasets/search/ranking_logs/v1/config.yaml`
- Create: `datasets/search/ranking_logs/v1/migrations/create_table.sql`
- Create: `datasets/search/ranking_logs/v1/migrations/__init__.py`
- Create: `datasets/search/ranking_logs/v1/README.md`
- Test: `ci_test/test_ranking_logs_dataset.py`

**Interfaces:**
- Consumes: ничего.
- Produces: `config.yaml` с блоками `table`, `dag`, `alerts`, `dq`, `feature_stats`, `spark`, `resources`, `dataset`; DDL из 40 колонок в фиксированном порядке — Task 3 обязан выдать `SELECT` ровно в этом порядке.

- [ ] **Step 1: Написать падающий тест**

Создать `ci_test/test_ranking_logs_dataset.py`:

```python
import re
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
CONFIG_PATH = ENTITY_DIR / "config.yaml"
DDL_PATH = ENTITY_DIR / "migrations/create_table.sql"

# Порядок колонок — контракт между DDL и SELECT'ом джоба: writeTo() сопоставляет
# их позиционно, поэтому расхождение молча перепутает значения местами.
EXPECTED_COLUMNS = [
    "collection_date",
    "event_date",
    "fired_at",
    "model_name",
    "request_id",
    "install_id",
    "search_query",
    "category_id",
    "promo_id",
    "position",
    "sku_group_id",
    "final_score",
    "model_probability",
    "alpha_component",
    "beta_component",
    "gamma_component",
    "delta_component",
    "dssm_score",
    "linear_score",
    "normalized_linear_score",
    "cpo_adv_percent",
    "bid_amount",
    "commission_percent",
    "seller_price",
    "logistics_fee",
    "cpi_cost",
    "cpm_bid",
    "cpo_percent",
    "vat_rate",
    "items_quantity",
    "alpha",
    "beta",
    "gamma",
    "delta",
    "sku_group_age_days",
    "product_rating",
    "total_reviews_count",
    "frequency_group",
    "users_total",
    "query_rank",
]

COLUMN_DEFINITION = re.compile(
    r"^\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+[A-Za-z]", re.MULTILINE
)


def load_config():
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def ddl_columns():
    body = DDL_PATH.read_text(encoding="utf-8")
    body = body[body.index("(") + 1 : body.index("\n)\nUSING iceberg")]
    return COLUMN_DEFINITION.findall(body)


def test_table_contract():
    table = load_config()["table"]
    assert table["catalog"] == "iceberg"
    assert table["schema"] == "silver"
    assert table["name"] == "feature_platform_ranking_logs_dataset_v1"
    assert table["primary_key"] == "collection_date,event_date,request_id,sku_group_id"
    assert table["meta"]["team"] == "team:search"


def test_dag_and_alert_contract():
    config = load_config()
    assert config["dag"]["schedule"] == "0 12 * * 0"
    assert config["dag"]["team"] == "search"
    assert config["alerts"]["team"] == "search"
    assert config["alerts"]["severity"] == "P4"
    assert config["alerts"]["oncall_webhook_conn_id"] == "oncall_webhook_search"


def test_dq_and_feature_stats_look_at_the_same_partition():
    config = load_config()
    dq = config["dq"]
    stats = config["feature_stats"]
    assert dq["partition_column"] == "collection_date"
    assert stats["partition_column"] == dq["partition_column"]
    assert stats["partition_date_template"] == dq["partition_date_template"]
    assert "data_interval_end" in dq["partition_date_template"]


def test_all_dq_tests_are_warn_and_growth_is_absent():
    tests = load_config()["dq"]["tests"]
    names = [test["name"] for test in tests]
    # row_count_growth берёт baseline как partition_date - 1 day (dq/tests.py),
    # у недельной партиции предыдущего дня не существует — тест бесполезен.
    assert "row_count_growth" not in names
    assert {"primary_key_not_null", "primary_key_unique", "freshness", "row_count_min"} <= set(names)
    for test in tests:
        assert test["severity"] == "warn", test["name"]


def test_dataset_parameters_are_declared_in_config():
    dataset = load_config()["dataset"]
    assert dataset["model_name"] == "search_unified_model_v9_cold_start"
    assert dataset["sample_percent"] == 7


def test_ddl_columns_match_the_agreed_order():
    assert ddl_columns() == EXPECTED_COLUMNS


def test_ddl_is_partitioned_by_collection_date():
    body = DDL_PATH.read_text(encoding="utf-8")
    assert "PARTITIONED BY (collection_date)" in body
    assert "'engine.hive.lock-enabled' = 'false'" in body


def test_readme_states_the_contract():
    readme = (ENTITY_DIR / "README.md").read_text(encoding="utf-8")
    assert "iceberg.silver.feature_platform_ranking_logs_dataset_v1" in readme
    assert "feature-platform.datasets.search.ranking_logs.v1" in readme
    assert "datasets/search/ranking_logs/v1" in readme
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `python3 -m pytest ci_test/test_ranking_logs_dataset.py -v`
Expected: FAIL — `FileNotFoundError` на `datasets/search/ranking_logs/v1/config.yaml`.

- [ ] **Step 3: Создать `config.yaml`**

```yaml
resources:
  path: ../../../../config/spark/resources.yaml

spark:
  template_path: ../../../../config/spark/layer_spark_application.yaml
  application_name: fetch-dataset-ranking-logs-v1
  main_application_file: local:///git/repo/datasets/search/ranking_logs/v1/entrypoints/get_ranking_logs_dataset.py
  resource_profile: search_dataset

table:
  key: dataset_ranking_logs_v1
  catalog: iceberg
  schema: silver
  name: feature_platform_ranking_logs_dataset_v1
  primary_key: collection_date,event_date,request_id,sku_group_id
  meta:
    team: team:search

# Параметры сбора. Джоб читает этот блок из config.yaml рядом с собой, чтобы
# значения не пришлось дублировать в общем spark-шаблоне: шаблон
# config/spark/layer_spark_application.yaml общий для всех энтити и передаёт
# только partition_start, partition_end и table_name.
dataset:
  # Одна модель на ран. В источнике их 11; на 2026-08-25 целевая дала 481 982
  # запроса за сутки.
  model_name: search_unified_model_v9_cold_start
  # Доля запросов (не строк-кандидатов): попавший в выборку запрос берётся
  # целиком, со всеми кандидатами. В среднем ~837 кандидатов на запрос, поэтому
  # 7% дают ~28 млн строк в сутки и ~200 млн за недельный ран.
  sample_percent: 7

dag:
  team: search
  group_tag: ranking-logs-dataset
  schedule: "0 12 * * 0"
  start_date: "2026-09-07T12:00:00Z"

alerts:
  team: search
  severity: P4
  oncall_webhook_conn_id: oncall_webhook_search

dq:
  trino_conn_id: trino_search
  # Партиция — collection_date, и она считается от data_interval_end: это дата
  # фактического запуска DAG'а (воскресенье), тогда как data_interval_start —
  # первый день собираемого окна. Оба шаблона ниже обязаны совпадать дословно.
  partition_column: collection_date
  partition_date_template: '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  tests:
    # Датасет событийных показов: первичный ключ задаёт гранулярность строки, а
    # не контракт для потребителей, и качество источника не должно будить
    # дежурного. Результат по-прежнему пишется в feature_platform_dq_results.
    - name: primary_key_not_null
      severity: warn
    - name: primary_key_unique
      severity: warn
    - name: freshness
      severity: warn
    - name: row_count_min
      severity: warn
    # row_count_growth сознательно отсутствует: dq/tests.py берёт baseline как
    # partition_date - 1 day, а у недельной партиции предыдущих суток не
    # существует, поэтому тест на каждом ране возвращал бы "нет baseline".
    - name: not_null
      columns: [final_score, sku_group_id]
      severity: warn

feature_stats:
  trino_conn_id: trino_search
  # Дословно совпадает с dq: обе таски обязаны смотреть на одну партицию.
  partition_column: collection_date
  partition_date_template: '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  exclude_columns:
    # Идентификаторы и свободный текст: профиль min/max/перцентилей по ним
    # бессмыслен.
    - sku_group_id
    - request_id
    - install_id
    - promo_id
    - search_query
```

- [ ] **Step 4: Создать `migrations/create_table.sql`**

```sql
CREATE TABLE IF NOT EXISTS {target_table} (
    collection_date DATE COMMENT 'Дата фактического запуска DAG в UTC (воскресенье); партиция таблицы',
    event_date DATE COMMENT 'Дата события из лога; в партиции ровно 7 значений, воскресенье-суббота',
    fired_at TIMESTAMP COMMENT 'Время запроса к ранжирующему сервису из ranking_analytics_events.fired_at',
    model_name STRING COMMENT 'Имя ранжирующей модели; фиксировано параметром dataset.model_name',
    request_id STRING COMMENT 'ID запроса к ранжирующему сервису; единица сэмплирования',
    install_id STRING COMMENT 'Install ID пользователя',
    search_query STRING COMMENT 'Поисковый запрос как в логе, без нормализации',
    category_id INT COMMENT 'ID категории запроса; для поисковой модели обычно NULL',
    promo_id STRING COMMENT 'Идентификатор промо-конфигурации ранжирования из лога',
    `position` INT COMMENT 'Позиция кандидата в массиве ranking_candidates, 1-based',
    sku_group_id BIGINT COMMENT 'Кандидат: ranking_candidates[position], уровень sku group',
    final_score DOUBLE COMMENT 'Итоговый скор формулы: final_scores[position], равен model_output[position][0]',
    model_probability DOUBLE COMMENT 'Вероятность модели: model_output[position][1]',
    alpha_component DOUBLE COMMENT 'Alpha-составляющая формулы: model_output[position][2]',
    beta_component DOUBLE COMMENT 'Beta-составляющая формулы: model_output[position][3]',
    gamma_component DOUBLE COMMENT 'Gamma-составляющая формулы: model_output[position][4]',
    delta_component DOUBLE COMMENT 'Delta-составляющая формулы: model_output[position][5]',
    dssm_score DOUBLE COMMENT 'external_features.dssm_score[position]',
    linear_score DOUBLE COMMENT 'external_features.linear_score[position]',
    normalized_linear_score DOUBLE COMMENT 'external_features.normalized_linear_score[position]',
    cpo_adv_percent DOUBLE COMMENT 'external_features.cpo_adv_percents[position]',
    bid_amount DOUBLE COMMENT 'external_features.bid_amounts[position]',
    commission_percent DOUBLE COMMENT 'Комиссия в процентах 0-100: cm2_features[position][0]',
    seller_price DOUBLE COMMENT 'Цена продажи, на которой считалась формула: cm2_features[position][1]',
    logistics_fee DOUBLE COMMENT 'Логистический сбор: cm2_features[position][2]',
    cpi_cost DOUBLE COMMENT 'CPI-cost: cm2_features[position][3]',
    cpm_bid DOUBLE COMMENT 'Размер CPM-ставки: cm2_features[position][4]',
    cpo_percent DOUBLE COMMENT 'Процент CPO-ставки: cm2_features[position][5]',
    vat_rate DOUBLE COMMENT 'Коэффициент НДС, по умолчанию 1.12: cm2_features[position][6]',
    items_quantity DOUBLE COMMENT 'Количество товаров для расчёта: cm2_features[position][7]',
    alpha DOUBLE COMMENT 'Коэффициент alpha запроса: common_external_features[alpha]',
    beta DOUBLE COMMENT 'Коэффициент beta запроса: common_external_features[beta]',
    gamma DOUBLE COMMENT 'Коэффициент gamma запроса: common_external_features[gamma]',
    delta DOUBLE COMMENT 'Коэффициент delta запроса: common_external_features[delta]',
    sku_group_age_days INT COMMENT 'Возраст sku group в днях на event_date: event_date минус дата создания самого старого sku группы',
    product_rating DOUBLE COMMENT 'Средний рейтинг sku group из feature_platform_sku_group_feedback_base_stats',
    total_reviews_count BIGINT COMMENT 'Число опубликованных отзывов sku group из feature_platform_sku_group_feedback_base_stats',
    frequency_group STRING COMMENT 'Группа частотности запроса HF/MF/LF; LF, если запрос не найден в справочнике',
    users_total BIGINT COMMENT 'Число пользователей запроса за 30 дней; NULL, если запрос не найден',
    query_rank BIGINT COMMENT 'Ранг запроса по частотности; NULL, если запрос не найден'
)
USING iceberg
COMMENT 'Training dataset v1: развёрнутый лог ранжирования запрос x кандидат для подбора параметров формулы'
PARTITIONED BY (collection_date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
```

- [ ] **Step 5: Создать пустой `migrations/__init__.py`**

```bash
touch datasets/search/ranking_logs/v1/migrations/__init__.py
```

- [ ] **Step 6: Написать `README.md`**

Обязательные по AGENTS.md факты: полное имя таблицы, DAG id, путь энтити, назначение. Дополнительно — окно, сэмплирование, источники обогащений и решения по DQ.

```markdown
# ranking_logs v1

Тренировочный датасет для офлайн-подбора параметров ранжирующей формулы поиска.
Разворачивает лог ранжирующего сервиса до уровня «запрос × кандидат» и добавляет
разложение формулы, её входы и внешние скоры.

- Таблица: `iceberg.silver.feature_platform_ranking_logs_dataset_v1`
- DAG: `feature-platform.datasets.search.ranking_logs.v1`
- Путь энтити: `datasets/search/ranking_logs/v1`
- Назначение: офлайн-подбор параметров формулы и анализ. В ranking-service,
  inference-сервисы и любой онлайн-контур не выгружается.

## Окно и партиция

DAG идёт раз в неделю, `0 12 * * 0` UTC. `data_interval` недельный, поэтому один
ран покрывает 7 календарных суток:

- `event_date ∈ [date(data_interval_start), date(data_interval_end) - 1]` —
  воскресенье…суббота включительно, последние закрытые сутки — вчерашние;
- `collection_date = date(data_interval_end)` — воскресенье фактического запуска,
  она же партиция таблицы.

`collection_date` считается от `data_interval_end`, а не от `data_interval_start`
как в `datasets/search/search_ranking/v1`. Шаблоны `dq` и `feature_stats`
используют тот же `data_interval_end`.

## Отбор

Одна модель на ран, имя в `config.yaml` → `dataset.model_name`, сейчас
`search_unified_model_v9_cold_start`.

Сэмплирование детерминированное и по запросу, а не по строке-кандидату:
`pmod(xxhash64(request_id), 10000) < dataset.sample_percent * 100`. Попавший в
выборку запрос берётся целиком, со всеми кандидатами, без обрезки по позиции.
Перезапуск за ту же неделю даёт ту же выборку.

Стратификации по `frequency_group` нет сознательно: случайный отбор по запросам
сохраняет долю HF/MF/LF такой, какая она в трафике, а формула подбирается именно
под реальный трафик. `frequency_group` остаётся колонкой, стратифицировать можно
на анализе.

Объём: ~837 кандидатов на запрос, ~482 тыс. запросов в сутки по целевой модели,
то есть при `sample_percent = 7` порядка 28 млн строк в сутки и ~200 млн за ран.

## Источники

| Что | Откуда |
|---|---|
| Лог ранжирования | `iceberg.silver.ranking_analytics_events` |
| Возраст sku group | `iceberg.silver.sku`, `min(created_at)` по `sku_group_id` |
| Рейтинг и отзывы | `iceberg.gold.feature_platform_sku_group_feedback_base_stats` |
| Частотность запроса | `iceberg.silver.search_queries_frequency_groups_30d` |

Все массивы источника выровнены 1:1 с `ranking_candidates` — проверено на 7424
событиях. Разворот идёт общим `posexplode(arrays_zip(...))`.

`model_input['input']` (145 признаков) в датасет не пишется: имён признаков в
логе нет, а для подбора параметров формулы нужны только её составляющие.
`model_output[0]` тоже не пишется отдельной колонкой — он равен
`final_scores[position]`.

`alpha`/`beta`/`gamma`/`delta` лежат в источнике дважды — в
`common_external_features` и в хвосте `cm2_features` (позиции 8–11) с теми же
значениями. Берётся `common_external_features` как явный request-level контракт.

Цена берётся из лога (`cm2_features[1]`, цена продажи), а не как средняя по
sku-группе из `silver.sku_eod`: формула считалась именно на цене из лога.

Если частотный справочник не знает запрос, `frequency_group = 'LF'`, а
`users_total` и `query_rank` — NULL. Это безопасный дефолт: на 2026-08-31 в LF
было 12,6 млн запросов против 9013 в MF и 1000 в HF.

## Качество

Все DQ-тесты в severity `warn` по политике репозитория для событийных датасетов:
первичный ключ здесь задаёт гранулярность строки, а не контракт для
потребителей. Результаты пишутся в `feature_platform_dq_results`.

`row_count_growth` явно отключен через `enabled: false`: `dq/tests.py` берёт baseline как
`partition_date - 1 day`, у недельной партиции предыдущих суток не существует,
и тест на каждом ране возвращал бы «нет baseline». Явное отключение необходимо,
потому что `dq/config.py` инъецирует базовые тесты: любой базовый тест,
отсутствующий из явного списка, получает severity `error` по умолчанию.

`feature_stats` считает профиль по той же партиции; идентификаторы и текст
(`sku_group_id`, `request_id`, `install_id`, `promo_id`, `search_query`)
исключены из профиля.
```

- [ ] **Step 7: Запустить тест и убедиться, что он проходит**

Run: `python3 -m pytest ci_test/test_ranking_logs_dataset.py -v`
Expected: PASS, 8 тестов.

- [ ] **Step 8: Прогнать CI-валидаторы конфигов**

Run:
```bash
python3 scripts/validate_dq_configs.py
python3 scripts/validate_feature_stats_configs.py
```
Expected: обе команды завершаются с кодом 0. Валидатор `dq` сверяет колонки теста
`not_null` с колонками миграций — `final_score` и `sku_group_id` в DDL есть.

- [ ] **Step 9: Коммит**

```bash
git add datasets/search/ranking_logs/v1/config.yaml \
        datasets/search/ranking_logs/v1/migrations \
        datasets/search/ranking_logs/v1/README.md \
        ci_test/test_ranking_logs_dataset.py
git commit -m "feat(datasets): контракт и DDL датасета ranking_logs v1"
```

---

### Task 2: Окно партиции и параметры сбора

**Files:**
- Create: `datasets/search/ranking_logs/v1/job/__init__.py`
- Create: `datasets/search/ranking_logs/v1/job/entities.py`
- Create: `datasets/search/ranking_logs/v1/job/arguments.py`
- Create: `datasets/search/ranking_logs/v1/job/settings.py`
- Create: `datasets/search/ranking_logs/v1/job/partition.py`
- Test: `ci_test/test_ranking_logs_partition.py`

**Interfaces:**
- Consumes: `config.yaml` из Task 1 (блок `dataset`).
- Produces:
  - `entities.Arguments(partition_start: str, partition_end: str, table_name: str)`
  - `entities.DatasetSettings(model_name: str, sample_percent: int)`
  - `settings.load_dataset_settings(config_path: str | Path) -> DatasetSettings`
  - `partition.parse_airflow_timestamp(value: str) -> datetime` (aware UTC)
  - `partition.collection_date(partition_end: str) -> datetime.date`
  - `partition.event_date_bounds(partition_start: str, partition_end: str) -> tuple[date, date]` — начало включительно, конец **исключительно**
  - `arguments.parse_arguments() -> Arguments`

- [ ] **Step 1: Написать падающие тесты**

Создать `ci_test/test_ranking_logs_partition.py`:

```python
import contextlib
import importlib.util
import sys
from datetime import date
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")


@contextlib.contextmanager
def _isolated_job_package():
    """Имя пакета `job` занято десятком энтити репозитория, и соседний тест
    (ci_test/test_query_id_features.py) кэширует в sys.modules своё
    `job.entities` без уборки. Снимаем чужой кэш на время загрузки и
    возвращаем его на место, чтобы не сломать ни себя, ни соседей."""
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "job" or name.startswith("job.")
    }
    for name in saved:
        del sys.modules[name]
    saved_path = list(sys.path)
    sys.path.insert(0, str(ENTITY_DIR))
    try:
        yield
    finally:
        sys.path[:] = saved_path
        for name in [n for n in sys.modules if n == "job" or n.startswith("job.")]:
            del sys.modules[name]
        sys.modules.update(saved)


def load_module(name):
    with _isolated_job_package():
        spec = importlib.util.spec_from_file_location(
            f"ranking_logs_{name}", ENTITY_DIR / "job" / f"{name}.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def test_parse_airflow_timestamp_accepts_supported_formats():
    partition = load_module("partition")

    values = [
        "2026-09-13T12:00:00",
        "2026-09-13T12:00:00+00:00",
        "2026-09-13T12:00:00Z",
        "2026-09-13 12:00:00+00:00",
        "2026-09-13 12:00:00",
    ]
    for value in values:
        parsed = partition.parse_airflow_timestamp(value)
        assert parsed.date().isoformat() == "2026-09-13"
        assert parsed.tzinfo is not None


def test_parse_airflow_timestamp_rejects_garbage():
    partition = load_module("partition")

    try:
        partition.parse_airflow_timestamp("13.09.2026")
    except ValueError:
        return
    raise AssertionError("ожидался ValueError на неподдерживаемом формате")


def test_collection_date_is_the_run_sunday():
    partition = load_module("partition")

    # Ран в воскресенье 2026-09-13 12:00 UTC закрывает окно, начавшееся
    # в воскресенье 2026-09-06 12:00 UTC.
    assert partition.collection_date("2026-09-13 12:00:00+00:00") == date(2026, 9, 13)


def test_event_date_bounds_cover_sunday_to_saturday():
    partition = load_module("partition")

    start, end = partition.event_date_bounds(
        "2026-09-06 12:00:00+00:00", "2026-09-13 12:00:00+00:00"
    )

    assert start == date(2026, 9, 6)
    # Верхняя граница исключительная: последний собираемый день — суббота 12-е.
    assert end == date(2026, 9, 13)
    assert (end - start).days == 7


def test_load_dataset_settings_matches_the_yaml_source_of_truth():
    settings_module = load_module("settings")

    settings = settings_module.load_dataset_settings(ENTITY_DIR / "config.yaml")
    raw = yaml.safe_load((ENTITY_DIR / "config.yaml").read_text(encoding="utf-8"))

    assert settings.model_name == raw["dataset"]["model_name"]
    assert settings.sample_percent == int(raw["dataset"]["sample_percent"])


def test_load_dataset_settings_rejects_out_of_range_percent(tmp_path):
    settings_module = load_module("settings")

    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        "dataset:\n  model_name: m\n  sample_percent: 0\n", encoding="utf-8"
    )

    try:
        settings_module.load_dataset_settings(bad_config)
    except ValueError:
        return
    raise AssertionError("ожидался ValueError на sample_percent вне (0, 100]")
```

- [ ] **Step 2: Запустить тесты и убедиться, что они падают**

Run: `python3 -m pytest ci_test/test_ranking_logs_partition.py -v`
Expected: FAIL — модулей `job/partition.py` и `job/settings.py` не существует.

- [ ] **Step 3: Создать `job/__init__.py` и `job/entities.py`**

```bash
touch datasets/search/ranking_logs/v1/job/__init__.py
```

`job/entities.py`:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class Arguments:
    partition_start: str
    partition_end: str
    table_name: str


@dataclass(frozen=True)
class DatasetSettings:
    model_name: str
    sample_percent: int
```

- [ ] **Step 4: Создать `job/partition.py`**

```python
from datetime import date, datetime, timezone
from typing import Tuple


def parse_airflow_timestamp(value: str) -> datetime:
    """Разбирает Airflow-таймстемп в aware UTC datetime."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    for candidate in (normalized, normalized.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    raise ValueError(
        "Unsupported partition timestamp format. "
        f"Expected Airflow ISO timestamp or YYYY-MM-DD HH:MM:SS, got {value!r}"
    )


def collection_date(partition_end: str) -> date:
    """Дата фактического запуска DAG'а: конец недельного интервала."""
    return parse_airflow_timestamp(partition_end).date()


def event_date_bounds(partition_start: str, partition_end: str) -> Tuple[date, date]:
    """Границы окна логов: начало включительно, конец исключительно.

    Недельный data_interval начинается и заканчивается в один и тот же час, поэтому
    календарные сутки окна — от даты его начала до даты его конца, не включая
    последнюю: она ещё не закрыта на момент запуска.
    """
    start = parse_airflow_timestamp(partition_start).date()
    end = parse_airflow_timestamp(partition_end).date()
    if end <= start:
        raise ValueError(
            f"partition_end must be after partition_start, got {partition_start!r} and {partition_end!r}"
        )
    return start, end
```

- [ ] **Step 5: Создать `job/settings.py`**

Читает блок `dataset` из `config.yaml` рядом с энтити. Собственный минимальный
парсер нужен потому, что наличие PyYAML в spark-образе не гарантировано; тест из
шага 1 сверяет результат с `yaml.safe_load`, так что разойтись они не могут.

```python
import os
from pathlib import Path
from typing import Dict, Union

from job.entities import DatasetSettings

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _read_block(config_path: Union[str, os.PathLike], block: str) -> Dict[str, str]:
    """Возвращает скалярные ключи одного верхнеуровневого блока config.yaml."""
    values: Dict[str, str] = {}
    inside = False
    with open(config_path, "r", encoding="utf-8") as config_file:
        for raw_line in config_file:
            if not raw_line.strip() or raw_line.lstrip().startswith("#"):
                continue
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            line = raw_line.strip()
            if indent == 0:
                inside = line.rstrip(":") == block
                continue
            if not inside:
                continue
            key, separator, value = line.partition(":")
            if separator and value.strip():
                values[key.strip()] = _unquote(value.strip())
    return values


def load_dataset_settings(
    config_path: Union[str, os.PathLike] = DEFAULT_CONFIG_PATH,
) -> DatasetSettings:
    block = _read_block(config_path, "dataset")

    model_name = block.get("model_name", "")
    if not model_name:
        raise ValueError(f"dataset.model_name is missing in {config_path}")

    sample_percent = int(block.get("sample_percent", 0))
    if not 0 < sample_percent <= 100:
        raise ValueError(
            f"dataset.sample_percent must be in (0, 100], got {sample_percent}"
        )

    return DatasetSettings(model_name=model_name, sample_percent=sample_percent)
```

- [ ] **Step 6: Создать `job/arguments.py`**

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

- [ ] **Step 7: Запустить тесты и убедиться, что они проходят**

Run: `python3 -m pytest ci_test/test_ranking_logs_partition.py -v`
Expected: PASS, 6 тестов.

- [ ] **Step 8: Коммит**

```bash
git add datasets/search/ranking_logs/v1/job ci_test/test_ranking_logs_partition.py
git commit -m "feat(datasets): окно партиции и параметры сбора ranking_logs v1"
```

---

### Task 3: Построитель SQL, джоб и entrypoint

**Files:**
- Create: `datasets/search/ranking_logs/v1/job/query.py`
- Create: `datasets/search/ranking_logs/v1/job/getting_ranking_logs_dataset.py`
- Create: `datasets/search/ranking_logs/v1/entrypoints/get_ranking_logs_dataset.py`
- Test: `ci_test/test_ranking_logs_query.py`

**Interfaces:**
- Consumes: `partition.event_date_bounds`, `partition.collection_date`, `settings.load_dataset_settings`, `entities.Arguments`, `entities.DatasetSettings`, DDL-порядок колонок из Task 1.
- Produces:
  - `query.HASH_BUCKETS: int = 10000`
  - `query.sample_threshold(sample_percent: int) -> int`
  - `query.build_dataset_query(collection_date: date, event_date_start: date, event_date_end: date, settings: DatasetSettings) -> str`
  - `getting_ranking_logs_dataset.build_ranking_logs_dataset(spark, partition_start, partition_end, settings)`
  - `getting_ranking_logs_dataset.save_ranking_logs_dataset(spark, partition_start, partition_end, target_table, settings)`
  - `getting_ranking_logs_dataset.run(spark, arguments)`

**Почему построитель SQL живёт в отдельном модуле:** pyspark в окружении, где
гоняется `ci_test`, не установлен (проверено: `import pyspark` → `ModuleNotFoundError`).
Модуль с текстом запроса не должен импортировать pyspark, иначе его нельзя
протестировать. Тот же приём уже применён в
`layers/silver/query_sku_group_id/search_query_sku_group_dssm_scores/v1/job/partition.py`.


**Изоляция пакета `job` при загрузке модулей в тестах:** Имя пакета `job` используется во всех энтити репозитория. Соседний тест `ci_test/test_query_id_features.py` кэширует своё `job.entities` в `sys.modules` без уборки, и при запуске тестов в естественном порядке это отравляет кэш для последующих тестов. Поэтому оба файла `test_ranking_logs_partition.py` и `test_ranking_logs_query.py` используют контекстный менеджер `_isolated_job_package()` для сохранения и восстановления кэша вокруг загрузки модулей — это гарантирует, что каждый тест работает с правильной версией пакета и не ломает соседей.

- [ ] **Step 1: Написать падающие тесты**

Создать `ci_test/test_ranking_logs_query.py`:

```python
import importlib.util
import re
import sys
from datetime import date
from pathlib import Path

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
DDL_PATH = ENTITY_DIR / "migrations/create_table.sql"

COLUMN_DEFINITION = re.compile(
    r"^\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+[A-Za-z]", re.MULTILINE
)


@contextlib.contextmanager
def _isolated_job_package():
    """Имя пакета `job` занято десятком энтити репозитория, и соседний тест
    (ci_test/test_query_id_features.py) кэширует в sys.modules своё
    `job.entities` без уборки. Снимаем чужой кэш на время загрузки и
    возвращаем его на место, чтобы не сломать ни себя, ни соседей."""
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "job" or name.startswith("job.")
    }
    for name in saved:
        del sys.modules[name]
    saved_path = list(sys.path)
    sys.path.insert(0, str(ENTITY_DIR))
    try:
        yield
    finally:
        sys.path[:] = saved_path
        for name in [n for n in sys.modules if n == "job" or n.startswith("job.")]:
            del sys.modules[name]
        sys.modules.update(saved)


def load_module(name):
    with _isolated_job_package():
        spec = importlib.util.spec_from_file_location(
            f"ranking_logs_{name}", ENTITY_DIR / "job" / f"{name}.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def load_settings():
    return load_module("settings").load_dataset_settings(ENTITY_DIR / "config.yaml")


def build_query():
    return load_module("query").build_dataset_query(
        collection_date=date(2026, 9, 13),
        event_date_start=date(2026, 9, 6),
        event_date_end=date(2026, 9, 13),
        settings=load_settings(),
    )


def ddl_columns():
    body = DDL_PATH.read_text(encoding="utf-8")
    body = body[body.index("(") + 1 : body.index("\n)\nUSING iceberg")]
    return COLUMN_DEFINITION.findall(body)


def final_select_aliases(query: str):
    """Алиасы верхнеуровневого SELECT: последний SELECT ... FROM в запросе."""
    tail = query[query.rindex("SELECT") :]
    projection = tail[: tail.index("\nFROM ")]
    aliases = []
    for line in projection.splitlines()[1:]:
        item = line.strip().rstrip(",")
        if not item or item.startswith("--"):
            continue
        aliases.append(re.split(r"\s+AS\s+", item)[-1].strip("`"))
    return aliases


def test_query_projects_ddl_columns_in_order():
    assert final_select_aliases(build_query()) == ddl_columns()


def test_query_filters_the_configured_model_only():
    assert "e.model_name = 'search_unified_model_v9_cold_start'" in build_query()


def test_query_samples_requests_deterministically():
    query = build_query()
    # sample_percent = 7 -> порог 700 из 10000. pmod, а не abs: xxhash64 может
    # вернуть Long.MIN_VALUE, у которого abs отрицателен и условие отсечёт всё.
    assert "pmod(xxhash64(e.request_id), 10000) < 700" in query
    assert "abs(xxhash64" not in query


def test_query_uses_the_seven_day_window():
    query = build_query()
    assert "DATE '2026-09-13' AS collection_date" in query
    assert "DATE '2026-09-06' AS event_date_start" in query
    assert "DATE '2026-09-13' AS event_date_end" in query


def test_query_never_reads_the_145_feature_vector():
    query = build_query()
    assert "model_input['input']" not in query
    assert "'$.input'" not in query


def test_query_explodes_all_aligned_arrays_together():
    query = build_query()
    # Один общий posexplode(arrays_zip(...)) — единственный способ гарантировать,
    # что кандидат и все его скоры взяты по одному индексу.
    assert query.count("posexplode") == 1
    for array_name in (
        "ranking_candidates",
        "final_scores",
        "model_output",
        "cm2_features",
        "dssm_scores",
        "linear_scores",
        "normalized_linear_scores",
        "cpo_adv_percents",
        "bid_amounts",
    ):
        assert array_name in query


def test_query_defaults_unknown_queries_to_low_frequency():
    assert "COALESCE(fq.frequency_group, 'LF')" in build_query()


def test_query_left_joins_every_enrichment():
    query = build_query()
    # Обогащения не должны терять строки лога: только LEFT JOIN.
    assert query.count("LEFT JOIN") == 3


def test_sample_threshold_scales_with_percent():
    query_module = load_module("query")
    assert query_module.sample_threshold(1) == 100
    assert query_module.sample_threshold(7) == 700
    assert query_module.sample_threshold(100) == 10000
```

- [ ] **Step 2: Запустить тесты и убедиться, что они падают**

Run: `python3 -m pytest ci_test/test_ranking_logs_query.py -v`
Expected: FAIL — `job/query.py` не существует.

- [ ] **Step 3: Создать `job/query.py`**

Каждая колонка финального `SELECT` обязана иметь явный `AS`: тест
`test_query_projects_ddl_columns_in_order` берёт алиас как текст после `AS`, и
колонка без него не совпадёт с именем в DDL.

```python
from datetime import date

from job.entities import DatasetSettings

HASH_BUCKETS = 10000


def sample_threshold(sample_percent: int) -> int:
    """Верхняя граница хэш-бакета для заданного процента запросов."""
    return int(sample_percent) * HASH_BUCKETS // 100


def build_dataset_query(
    collection_date: date,
    event_date_start: date,
    event_date_end: date,
    settings: DatasetSettings,
) -> str:
    threshold = sample_threshold(settings.sample_percent)

    return f"""
WITH params AS (
    SELECT
        DATE '{collection_date.isoformat()}' AS collection_date,
        DATE '{event_date_start.isoformat()}' AS event_date_start,
        DATE '{event_date_end.isoformat()}' AS event_date_end
),
event_dates AS (
    SELECT explode(sequence(p.event_date_start, date_sub(p.event_date_end, 1))) AS event_date
    FROM params p
),
sampled_events AS (
    SELECT
        CAST(e.fired_at AS DATE) AS event_date,
        e.fired_at AS fired_at,
        e.model_name AS model_name,
        e.request_id AS request_id,
        e.install_id AS install_id,
        e.search_query AS search_query,
        e.category_id AS category_id,
        e.promo_id AS promo_id,
        e.ranking_candidates AS ranking_candidates,
        e.final_scores AS final_scores,
        e.model_output AS model_output,
        e.model_input['cm2_features'] AS cm2_features,
        e.common_external_features AS common_external_features,
        COALESCE(
            from_json(get_json_object(e.external_features, '$.dssm_score'), 'ARRAY<DOUBLE>'),
            array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        ) AS dssm_scores,
        COALESCE(
            from_json(get_json_object(e.external_features, '$.linear_score'), 'ARRAY<DOUBLE>'),
            array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        ) AS linear_scores,
        COALESCE(
            from_json(get_json_object(e.external_features, '$.normalized_linear_score'), 'ARRAY<DOUBLE>'),
            array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        ) AS normalized_linear_scores,
        COALESCE(
            from_json(get_json_object(e.external_features, '$.cpo_adv_percents'), 'ARRAY<DOUBLE>'),
            array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        ) AS cpo_adv_percents,
        COALESCE(
            from_json(get_json_object(e.external_features, '$.bid_amounts'), 'ARRAY<DOUBLE>'),
            array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        ) AS bid_amounts
    FROM iceberg.silver.ranking_analytics_events e
    CROSS JOIN params p
    WHERE
        e.fired_at >= p.event_date_start
        AND e.fired_at < p.event_date_end
        AND e.model_name = '{settings.model_name}'
        AND e.ranking_candidates IS NOT NULL
        AND size(e.ranking_candidates) > 0
        AND e.model_input IS NOT NULL
        AND size(e.model_input['cm2_features']) = size(e.ranking_candidates)
        -- Отбор по запросу, не по строке: попавший запрос берётся со всеми
        -- кандидатами. pmod, а не abs: xxhash64 может вернуть Long.MIN_VALUE,
        -- у которого abs отрицателен и условие молча отсечёт всё.
        AND pmod(xxhash64(e.request_id), {HASH_BUCKETS}) < {threshold}
),
candidates AS (
    SELECT
        s.event_date AS event_date,
        s.fired_at AS fired_at,
        s.model_name AS model_name,
        s.request_id AS request_id,
        s.install_id AS install_id,
        s.search_query AS search_query,
        s.category_id AS category_id,
        s.promo_id AS promo_id,
        s.common_external_features AS common_external_features,
        candidate_index + 1 AS candidate_position,
        candidate AS candidate
    FROM sampled_events s
    LATERAL VIEW posexplode(
        arrays_zip(
            s.ranking_candidates,
            s.final_scores,
            s.model_output,
            s.cm2_features,
            s.dssm_scores,
            s.linear_scores,
            s.normalized_linear_scores,
            s.cpo_adv_percents,
            s.bid_amounts
        )
    ) exploded AS candidate_index, candidate
),
sku_group_created AS (
    SELECT
        sku_group_id,
        MIN(created_at) AS created_at
    FROM iceberg.silver.sku
    WHERE sku_group_id IS NOT NULL
    GROUP BY sku_group_id
),
feedback_snapshot_dates AS (
    SELECT DISTINCT f.date AS snapshot_date
    FROM iceberg.gold.feature_platform_sku_group_feedback_base_stats f
    CROSS JOIN params p
    WHERE
        f.date < p.event_date_end
        AND f.date >= date_sub(p.event_date_start, 30)
),
feedback_date_map AS (
    SELECT
        d.event_date AS event_date,
        MAX(s.snapshot_date) AS snapshot_date
    FROM event_dates d
    JOIN feedback_snapshot_dates s ON s.snapshot_date <= d.event_date
    GROUP BY d.event_date
),
feedback AS (
    SELECT
        m.event_date AS event_date,
        f.sku_group_id AS sku_group_id,
        f.product_rating AS product_rating,
        f.total_reviews_count AS total_reviews_count
    FROM feedback_date_map m
    JOIN iceberg.gold.feature_platform_sku_group_feedback_base_stats f
        ON f.date = m.snapshot_date
),
frequency_snapshot_dates AS (
    SELECT DISTINCT g.analyze_date AS snapshot_date
    FROM iceberg.silver.search_queries_frequency_groups_30d g
    CROSS JOIN params p
    WHERE
        g.analyze_date < p.event_date_end
        AND g.analyze_date >= date_sub(p.event_date_start, 30)
),
frequency_date_map AS (
    SELECT
        d.event_date AS event_date,
        MAX(s.snapshot_date) AS snapshot_date
    FROM event_dates d
    JOIN frequency_snapshot_dates s ON s.snapshot_date <= d.event_date
    GROUP BY d.event_date
),
frequency AS (
    SELECT
        m.event_date AS event_date,
        trim(lower(g.query_text)) AS query,
        g.frequency_group AS frequency_group,
        g.users_total AS users_total,
        g.query_rank AS query_rank
    FROM frequency_date_map m
    JOIN iceberg.silver.search_queries_frequency_groups_30d g
        ON g.analyze_date = m.snapshot_date
)
SELECT
    DATE '{collection_date.isoformat()}' AS collection_date,
    c.event_date AS event_date,
    c.fired_at AS fired_at,
    c.model_name AS model_name,
    c.request_id AS request_id,
    c.install_id AS install_id,
    c.search_query AS search_query,
    c.category_id AS category_id,
    c.promo_id AS promo_id,
    CAST(c.candidate_position AS INT) AS `position`,
    CAST(c.candidate.ranking_candidates AS BIGINT) AS sku_group_id,
    CAST(c.candidate.final_scores AS DOUBLE) AS final_score,
    CAST(c.candidate.model_output[1] AS DOUBLE) AS model_probability,
    CAST(c.candidate.model_output[2] AS DOUBLE) AS alpha_component,
    CAST(c.candidate.model_output[3] AS DOUBLE) AS beta_component,
    CAST(c.candidate.model_output[4] AS DOUBLE) AS gamma_component,
    CAST(c.candidate.model_output[5] AS DOUBLE) AS delta_component,
    CAST(c.candidate.dssm_scores AS DOUBLE) AS dssm_score,
    CAST(c.candidate.linear_scores AS DOUBLE) AS linear_score,
    CAST(c.candidate.normalized_linear_scores AS DOUBLE) AS normalized_linear_score,
    CAST(c.candidate.cpo_adv_percents AS DOUBLE) AS cpo_adv_percent,
    CAST(c.candidate.bid_amounts AS DOUBLE) AS bid_amount,
    CAST(c.candidate.cm2_features[0] AS DOUBLE) AS commission_percent,
    CAST(c.candidate.cm2_features[1] AS DOUBLE) AS seller_price,
    CAST(c.candidate.cm2_features[2] AS DOUBLE) AS logistics_fee,
    CAST(c.candidate.cm2_features[3] AS DOUBLE) AS cpi_cost,
    CAST(c.candidate.cm2_features[4] AS DOUBLE) AS cpm_bid,
    CAST(c.candidate.cm2_features[5] AS DOUBLE) AS cpo_percent,
    CAST(c.candidate.cm2_features[6] AS DOUBLE) AS vat_rate,
    CAST(c.candidate.cm2_features[7] AS DOUBLE) AS items_quantity,
    CAST(c.common_external_features['alpha'] AS DOUBLE) AS alpha,
    CAST(c.common_external_features['beta'] AS DOUBLE) AS beta,
    CAST(c.common_external_features['gamma'] AS DOUBLE) AS gamma,
    CAST(c.common_external_features['delta'] AS DOUBLE) AS delta,
    CAST(datediff(c.event_date, CAST(sg.created_at AS DATE)) AS INT) AS sku_group_age_days,
    CAST(fb.product_rating AS DOUBLE) AS product_rating,
    CAST(fb.total_reviews_count AS BIGINT) AS total_reviews_count,
    COALESCE(fq.frequency_group, 'LF') AS frequency_group,
    CAST(fq.users_total AS BIGINT) AS users_total,
    CAST(fq.query_rank AS BIGINT) AS query_rank
FROM candidates c
LEFT JOIN sku_group_created sg
    ON sg.sku_group_id = c.candidate.ranking_candidates
LEFT JOIN feedback fb
    ON fb.event_date = c.event_date
    AND fb.sku_group_id = c.candidate.ranking_candidates
LEFT JOIN frequency fq
    ON fq.event_date = c.event_date
    AND fq.query = trim(lower(c.search_query))
"""
```

- [ ] **Step 4: Создать `job/getting_ranking_logs_dataset.py`**

```python
from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments, DatasetSettings
from job.partition import collection_date as run_collection_date
from job.partition import event_date_bounds
from job.query import build_dataset_query
from job.settings import load_dataset_settings


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_ranking_logs_dataset(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    settings: DatasetSettings,
):
    event_date_start, event_date_end = event_date_bounds(partition_start, partition_end)
    return spark.sql(
        build_dataset_query(
            collection_date=run_collection_date(partition_end),
            event_date_start=event_date_start,
            event_date_end=event_date_end,
            settings=settings,
        )
    )


def save_ranking_logs_dataset(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
    settings: DatasetSettings,
) -> None:
    dataset = build_ranking_logs_dataset(spark, partition_start, partition_end, settings)

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    dataset.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_ranking_logs_dataset(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
        load_dataset_settings(),
    )
```

- [ ] **Step 5: Создать `entrypoints/get_ranking_logs_dataset.py`**

```python
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from job.arguments import parse_arguments
from job.getting_ranking_logs_dataset import run


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("getting-ranking-logs-dataset-v1")
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

Run: `python3 -m pytest ci_test/test_ranking_logs_query.py -v`
Expected: PASS, 9 тестов. Если `test_query_projects_ddl_columns_in_order` падает —
чинить порядок и алиасы в `SELECT`, а не в DDL: DDL согласован с заказчиком в Task 1.

- [ ] **Step 7: Проверить, что SQL разбирается Spark'ом**

Тесты выше текстовые и синтаксическую ошибку не поймают. Прогнать разбор в том же
образе, что и CI-миграции:

```bash
docker run --rm -v "$PWD":/repo -w /repo \
  ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3 \
  python3 -c "
import sys
sys.path.insert(0, 'datasets/search/ranking_logs/v1')
from datetime import date
from job.query import build_dataset_query
from job.settings import load_dataset_settings
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').appName('parse-check').getOrCreate()
query = build_dataset_query(date(2026,9,13), date(2026,9,6), date(2026,9,13), load_dataset_settings('datasets/search/ranking_logs/v1/config.yaml'))
spark.sql('EXPLAIN ' + query)
print('parsed ok')
"
```

Expected: в выводе `parsed ok`. Исключение `AnalysisException: Table or view not found`
на этом шаге ожидаемо и означает, что синтаксис разобран: таблиц в локальном Spark
нет. `ParseException` — настоящая синтаксическая ошибка, её надо чинить.

- [ ] **Step 8: Коммит**

```bash
git add datasets/search/ranking_logs/v1/job/query.py \
        datasets/search/ranking_logs/v1/job/getting_ranking_logs_dataset.py \
        datasets/search/ranking_logs/v1/entrypoints \
        ci_test/test_ranking_logs_query.py
git commit -m "feat(datasets): построитель SQL, джоб и entrypoint ranking_logs v1"
```

---

### Task 4: DAG, конфиг-фабрика и регистрация в карте платформы

**Files:**
- Create: `datasets/search/ranking_logs/v1/dag.py`
- Create: `datasets/search/ranking_logs/v1/config/__init__.py`
- Create: `datasets/search/ranking_logs/v1/config/factory.py`
- Modify: `docs/feature_platform_map.md` (регенерируется скриптом)
- Test: `ci_test/test_ranking_logs_dag.py`

**Interfaces:**
- Consumes: `config.yaml` (Task 1), entrypoint (Task 3), `dq.task.build_dq_task`, `feature_stats.task.build_feature_stats_task`.
- Produces: DAG `feature-platform.datasets.search.ranking_logs.v1` с графом `collect_ranking_logs_dataset >> [dq_task, stats_task]`.

- [ ] **Step 1: Написать падающие тесты**

Создать `ci_test/test_ranking_logs_dag.py`:

```python
import ast
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
DAG_PATH = ENTITY_DIR / "dag.py"


def dag_source():
    return DAG_PATH.read_text(encoding="utf-8")


def test_dag_id_matches_the_entity_path():
    assert 'dag_id="feature-platform.datasets.search.ranking_logs.v1"' in dag_source()


def test_dag_builds_dq_and_feature_stats_tasks():
    tree = ast.parse(dag_source())
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "build_dq_task" in called
    assert "build_feature_stats_task" in called


def test_dq_and_stats_are_terminal_and_parallel():
    source = dag_source()
    assert ">> [dq_task, stats_task]" in source
    assert "dq_task >>" not in source
    assert "stats_task >>" not in source


def test_dag_partition_template_matches_the_config():
    source = dag_source()
    config = yaml.safe_load((ENTITY_DIR / "config.yaml").read_text(encoding="utf-8"))
    template = config["dq"]["partition_date_template"]
    # Шаблон в DAG'е и в конфиге обязан быть одним и тем же: иначе dq посчитает
    # тесты по другой партиции, чем записал джоб.
    assert template in source


def test_dag_is_paused_on_creation_and_single_run():
    source = dag_source()
    assert "is_paused_upon_creation=True" in source
    assert "max_active_runs=1" in source


def test_dag_is_tagged_as_dataset():
    assert '"dataset"' in dag_source()
```

- [ ] **Step 2: Запустить тесты и убедиться, что они падают**

Run: `python3 -m pytest ci_test/test_ranking_logs_dag.py -v`
Expected: FAIL — `datasets/search/ranking_logs/v1/dag.py` не существует.

- [ ] **Step 3: Скопировать конфиг-фабрику**

Фабрика читает `config.yaml` рядом с собой и не содержит ничего специфичного для
энтити, поэтому копируется без изменений — это установившийся в репозитории
паттерн (у каждой энтити свой `config/factory.py`).

```bash
mkdir -p datasets/search/ranking_logs/v1/config
touch datasets/search/ranking_logs/v1/config/__init__.py
cp datasets/search/search_ranking/v1/config/factory.py \
   datasets/search/ranking_logs/v1/config/factory.py
```

Затем поменять в скопированном файле дефолт `group_tag` и `schedule`, чтобы
дефолты не тянули чужие значения, если ключа в конфиге вдруг не окажется:

```python
        "group_tag": str(dag_config.get("group_tag", "ranking-logs-dataset")),
        "schedule": str(dag_config.get("schedule", "0 12 * * 0")),
```

- [ ] **Step 4: Создать `dag.py`**

```python
import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sdk import dag
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

REPO_ROOT = os.path.abspath(os.path.join(DAG_DIR, "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

CONFIG_PATH = os.path.join(DAG_DIR, "config.yaml")
# Партиция считается от конца недельного интервала: это дата фактического запуска
# DAG'а, её же пишет джоб в collection_date. Значение обязано совпадать с
# dq.partition_date_template и feature_stats.partition_date_template в config.yaml.
DQ_PARTITION_DATE = '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'

from config.factory import get_dag_settings, get_deployment

dag_settings = get_dag_settings()

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")

# DAG собирает недельный датасет логов ранжирования для подбора параметров формулы
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
    tags=["spark", "feature-platform", dag_settings["team_tag"], dag_settings["group_tag"], "dataset"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], "UTC"),
    start_date=pendulum.parse(dag_settings["start_date"]).in_timezone("UTC"),
    dag_id="feature-platform.datasets.search.ranking_logs.v1",
)
def collect_ranking_logs_dataset_v1():
    collect_dataset = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="collect_ranking_logs_dataset",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_dataset_ranking_logs_v1.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: downstream ждёт
    # таску dq, поэтому падение профилей не блокирует потребителей.
    collect_dataset >> [dq_task, stats_task]


dag = collect_ranking_logs_dataset_v1()
```

- [ ] **Step 5: Запустить тесты и убедиться, что они проходят**

Run: `python3 -m pytest ci_test/test_ranking_logs_dag.py -v`
Expected: PASS, 6 тестов.

- [ ] **Step 6: Перегенерировать карту платформы**

Новый DAG обязан попасть в `docs/feature_platform_map.md`, иначе шаг CI
`validate feature platform map` упадёт.

Run:
```bash
python3 scripts/generate_feature_platform_map.py
python3 scripts/generate_feature_platform_map.py --check
```
Expected: первая команда переписывает `docs/feature_platform_map.md`, вторая
завершается с кодом 0. В diff'е карты должен появиться
`feature-platform.datasets.search.ranking_logs.v1` со шагами `dq` и `feature_stats`.

- [ ] **Step 7: Прогнать весь набор проверок, которые гоняет CI**

Run:
```bash
python3 scripts/validate_dq_configs.py
python3 scripts/validate_feature_stats_configs.py
python3 scripts/validate_ranking_upload_configs.py
python3 scripts/generate_feature_platform_map.py --check
python3 -m pytest ci_test -q
git diff --check
```
Expected: валидаторы и `--check` — код 0; pytest — новые тесты зелёные, число
падений не больше, чем было на `master` до начала работы (зафиксировать это
число до старта: `git stash && python3 -m pytest ci_test -q; git stash pop`).

- [ ] **Step 8: Проверить, что миграция раскатывается Spark'ом**

Run:
```bash
docker run --rm -v "$PWD":/repo -w /repo \
  ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3 \
  /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  scripts/run_pyspark_migrations.py --repo-root . --validation-mode \
  --validation-warehouse /tmp/feature-platform-migration-warehouse
```
Expected: код 0, в логе — создание `feature_platform_ranking_logs_dataset_v1`.

- [ ] **Step 9: Коммит**

```bash
git add datasets/search/ranking_logs/v1/dag.py \
        datasets/search/ranking_logs/v1/config \
        docs/feature_platform_map.md \
        ci_test/test_ranking_logs_dag.py
git commit -m "feat(datasets): DAG ranking_logs v1 и регистрация в карте платформы"
```

---

## После реализации: проверки на живых данных

Эти проверки требуют доступа к Trino (MCP-сервер `trino` на момент написания
плана был отключён) и выполняются до снятия DAG'а с паузы.

- [ ] **Совпадают ли `cm2_features[5]` и `external_features.cpo_adv_percents`,
  `cm2_features[4]` и `external_features.bid_amounts`.** Если совпадают на всех
  строках — убрать дублирующие колонки `cpo_percent` и `cpm_bid` отдельной
  миграцией, а не задним числом в `create_table.sql`.

```sql
SELECT
  count(*) AS n,
  count_if(cm2.cpo = ef.cpo) AS cpo_equal,
  count_if(cm2.bid = ef.bid) AS bid_equal
FROM (
  SELECT
    model_input['cm2_features'][1][6] AS cpo,
    model_input['cm2_features'][1][5] AS bid,
    CAST(json_extract(external_features, '$.cpo_adv_percents') AS array(double))[1] AS ef_cpo,
    CAST(json_extract(external_features, '$.bid_amounts') AS array(double))[1] AS ef_bid
  FROM "dwh-iceberg".silver.ranking_analytics_events
  WHERE fired_at >= timestamp '2026-08-25 12:00:00 UTC'
    AND fired_at < timestamp '2026-08-25 12:10:00 UTC'
    AND model_name = 'search_unified_model_v9_cold_start'
) t
```

- [ ] **Есть ли ежедневные партиции `feature_platform_sku_group_feedback_base_stats`.**
  Если в окне есть дни без партиции, fallback «последняя `date <= event_date`»
  уже реализован; проверка нужна, чтобы знать, насколько часто он срабатывает.

```sql
SELECT date, count(*) AS rows
FROM "dwh-iceberg".gold.feature_platform_sku_group_feedback_base_stats
WHERE date >= current_date - interval '14' day
GROUP BY date ORDER BY date
```

- [ ] **Первый ран под наблюдением.** Снять DAG с паузы, дождаться прогона,
  зафиксировать: длительность, число строк в партиции, фактические ресурсы Spark.
  При нехватке ресурсов завести отдельный профиль в `config/spark/resources.yaml`
  вместо `search_dataset` и обновить `spark.resource_profile` в `config.yaml`.
- [ ] **Порог `row_count_min`.** После двух-трёх ранов выставить его по реальной
  истории вместо дефолтного `min_rows: 0`.
