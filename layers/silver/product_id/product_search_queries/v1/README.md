# Silver-витрина поисковых запросов по Product ID

DAG id: `feature-platform.layers.silver.product_id.product_search_queries`.

Airflow group tag: `product-search-queries`.

Пайплайн строит дневной snapshot поисковых запросов, в которых `product_id` попадал в `ranking_candidates` модели `search_unified_model_v6`.

Целевая таблица: `iceberg.silver.feature_platform_product_search_queries`.

## Контракт

Путь сущности: `layers/silver/product_id/product_search_queries/v1`.

Грейн таблицы: одна строка на `date, product_id`.

Primary key: `date,product_id`.

Колонки:

- `date` - UTC-дата snapshot, равная дате Airflow `data_interval_end`;
- `product_id` - ID product из `iceberg.silver.sku`;
- `search_queries` - массив строк с текстами top-200 запросов, отсортированный по внутреннему `uniq_installs DESC`, затем по `search_query ASC` для стабильного порядка. Само значение `uniq_installs` в таблицу не пишется.

## Источники и логика

Источники:

- `iceberg.silver.ranking_analytics_events` - события ranking analytics;
- `iceberg.silver.sku` - маппинг `sku_group_id -> product_id`.

Те же источники, модель, длина окна и лимит запросов зафиксированы в `config.yaml` в блоке `source`: `engine: spark_iceberg`, `model_name: search_unified_model_v6`, `lookback_days: 14`, `top_queries_limit: 200`.

Окно расчета привязано к Airflow interval: `fired_at >= data_interval_end - 14 days` и `fired_at < data_interval_end` в UTC. Для ежедневного запуска `2026-06-29 03:00 UTC` snapshot получает `date = 2026-06-29`.

Пайплайн фильтрует события по `model_name = 'search_unified_model_v6'`, непустому `ranking_candidates`, разворачивает массив кандидатов в `sku_group_id`, соединяет его с `iceberg.silver.sku` и оставляет уникальные пары `search_query, product_id`. `uniq_installs` считается как `COUNT(DISTINCT install_id)` по `search_query` за то же 14-дневное окно, как в исходном SQL, и используется только для сортировки массива `search_queries`. После сортировки в таблицу попадают только первые 200 запросов для каждого `product_id`.

Строки без маппинга на непустой `product_id` не попадают в результат, чтобы не нарушать primary key. `search_query` не нормализуется и не фильтруется дополнительно, чтобы сохранить семантику источника из запроса.

## Оркестрация

DAG `feature-platform.layers.silver.product_id.product_search_queries` запускается ежедневно в `03:00 UTC`, стартовая дата - `2026-06-29 00:00 UTC`, чтобы поддержать snapshot за текущую дату `2026-06-29`. Каждый запуск пересчитывает один дневной snapshot и перезаписывает только партицию `date = data_interval_end::date` через `overwritePartitions()`, а не всю таблицу целиком.

Config orchestration contract: `dag.id = feature-platform.layers.silver.product_id.product_search_queries`, `dag.group_tag = product-search-queries`, `dag.schedule = "0 3 * * *"`, `dag.start_date = "2026-06-29T00:00:00Z"`.

В этой версии не добавлены отдельные upstream DQ sensors: оба источника используются как внешние Iceberg-таблицы, а подтвержденный контракт пришел из задачи. Таблица получает стандартные сгенерированные dbt DQ-тесты по primary key после синка источников.

Для окружений, где таблица уже была создана со старым типом `search_queries ARRAY<STRUCT<search_query, uniq_installs>>`, миграция `20260630_migrate_search_queries_to_array_string.sql` переименовывает старую колонку в `search_queries_with_installs` и добавляет новую `search_queries ARRAY<STRING>`. Job выравнивает запись под фактическую схему таблицы, поэтому backup-колонка заполняется `NULL` для новых партиций.

В `config.yaml` сейчас задано `table.meta.create_dbt_pr: false` и `table.meta.create_maintenance_pr: true`: CI не создает missing PR в dbt sources для этой таблицы, но добавляет таблицу в Iceberg maintenance, если записи еще нет.

Пайплайн использует общий Spark image и `git-sync`; отдельный Docker image для сущности не собирается. Spark resource profile: `large`.
