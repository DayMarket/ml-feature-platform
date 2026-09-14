# iceberg.gold.feature_platform_query_category_relevance_expanded

Релевантность категории поисковому запросу, расширенная всеми формулировками запроса.
Исходная витрина `iceberg.gold.feature_platform_query_category_relevance` хранит одну
формулировку на `query_id`, а ranking-service ищет признак по тексту запроса. Эта таблица
раскладывает метку на все формулировки того же `query_id` и служит источником набора
`query_category_relevance` модели `search_unified_model_clusters` (коллекция
`SKU_GROUP_CATEGORY_TO_QUERY`).

## Выход

- Таблица: `iceberg.gold.feature_platform_query_category_relevance_expanded`.
- Ключ: `date, category_id, query_text`; партиционирование по `date`.
- Запись: `overwritePartitions` партиции `date` = дата прогона. Перезапуск дня идемпотентен.

| Колонка | Тип | Описание |
|---|---|---|
| `date` | `DATE` | дата прогона (UTC, `data_interval_start`) |
| `category_id` | `BIGINT` | категория-кандидат |
| `query_text` | `STRING` | `lower()` от формулировки запроса |
| `relevance` | `INT` | метка `0` / `1` / `2`, NULL при выгрузке уходит как `0.0` |

## Источники

- `iceberg.gold.feature_platform_query_category_relevance` — метка на грейне
  `date, category_id, query_text` плюс `query_id`. Наполняется отдельным процессом.
- `iceberg.gold.feature_platform_search_query_id` — справочник `query_text → query_id`.
  Берутся все строки, без фильтра по `version`.

## Логика

`D` — дата `data_interval_start` в UTC. SQL — `render_query` в
`job/getting_query_category_relevance_expanded.py`.

1. Строки витрины с `date <= D`, чтобы перезапуск старого дня не видел более свежие данные.
2. К ним добавляются копии, где `query_text` заменён на каждую формулировку того же
   `query_id` из справочника (`JOIN` по `query_id`). Исходные строки остаются, в том числе
   те, чьего `query_id` нет в справочнике.
3. Итоговый `query_text` приводится к нижнему регистру (`lower`). Других преобразований
   нет: ни trim, ни схлопывания пробелов, ни замены `ё`.
4. На пару `category_id, query_text` остаётся одна строка:
   `row_number() over (partition by category_id, lower(query_text) order by date desc, relevance desc nulls last) = 1`.
   Равная `date` с разным `relevance` бывает, когда в одной категории два `query_id`
   отличаются только регистром (на 2026-09-14 — 5 пар, `faberlic` = 2 и `Faberlic` = 0):
   берётся максимальная метка.

Замер в Trino на 2026-09-14: витрина — 2,17 млн строк, после шага 2 — 37,0 млн строк,
в таблицу уходит 27,6 млн пар.

## Оркестрация

- DAG id: `feature-platform.layers.gold.category_id_query_text.query_category_relevance_expanded`
  (`layers/gold/category_id_query_text/query_category_relevance_expanded/v1/dag.py`).
- Групповой тег Airflow: `query-category-relevance` (общий с исходной витриной и upload'ом).
- Расписание: ежедневно в 03:30 UTC, `30 3 * * *`, `start_date=2026-09-15T00:00:00Z`,
  `catchup=False`, `max_active_runs=1`, DAG создаётся на паузе.
- Сенсоры:
  - таска `materialize` DAG'а
    `feature-platform.layers.gold.category_id_query_text.query_category_relevance`,
    delta 30 минут (прогон 03:00 той же даты). Таски `dq` у того DAG'а нет;
  - таска `dq` DAG'а `feature-platform.layers.gold.query_text_version.search_query_id`,
    delta 22 ч 30 мин: справочник стартует в 05:00, позже этого DAG'а, поэтому ждём прогон
    предыдущей даты.
- Таски: `getting_query_category_relevance_expanded` → параллельно `dq` и `feature_stats`.

## DQ и feature_stats

- `dq`: базовый набор. `primary_key_not_null` и `primary_key_unique` в `error` — это витрина
  признаков. `freshness`, `row_count_min`, `row_count_growth` в `warn`, пока у таблицы нет
  истории для порогов.
- `feature_stats`: профилируется одна колонка `relevance`, один скан партиции
  (~27,6 млн строк) в Trino в день.
- `partition_date_template` у обоих блоков совпадает с `DQ_PARTITION_DATE` в `dag.py`.

## Рантайм

Spark на общем шаблоне `config/spark/layer_spark_application.yaml`, профиль `medium`,
код доставляется `git-sync`, образ по умолчанию.

## Выгрузка

Upload `feature-platform.upload.query_category_relevance_upload`
(`upload/query_category_relevance_upload/v1`) ждёт таску `dq` этого DAG'а и публикует
партицию `date = {{ ds }}`: `category_id` уходит ключом `skuGroupCategoryId`,
`query_text` — ключом `query`, `relevance` — единственным признаком.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
