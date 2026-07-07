# Search Query/SKU Group Elasticsearch Features

Пайплайн собирает silver-признаки из Elasticsearch `_explanation` на уровне поискового запроса и `sku_group_id`.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_search_query_sku_group_es_features`.
- DAG: `feature-platform.layers.silver.query_sku_group_id.search_query_sku_group_es_features`.
- Путь: `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1`.
- Групповой тег Airflow: `search-es-features`.
- Расписание: ежедневно в 04:00 UTC, `0 4 * * *`.
- `start_date=2026-03-13T00:00:00Z`, `catchup=False`.

## Грейн / ключ

`date, query, sku_group_id`.

`date` - закрытый UTC-день, равный `data_interval_end - 1 day`. Эта дата используется как
`event_date` для dataset-таблицы и как `log_date` результата.

## Источники

- `iceberg.silver.feature_platform_dataset_search_ranking_v1` - repository-managed dataset с парами
  `query, sku_group_id`; DAG ждет сборочный DAG `feature-platform.datasets.search.search_ranking.v1`.
- `"dwh-iceberg".silver.search_logs` - внешний Trino-источник для `result_query_text`; отдельный sensor не
  ставится по подтвержденному контракту.
- Elasticsearch endpoint из Airflow connection `elasticsearch_search`, path `/_search`.

## Логика

Trino-шаг выбирает `query, sku_group_id` из dataset-таблицы за `event_date = date`, нормализуя запрос как
`lower(trim(replace(query, 'ё', 'е')))`.

`search_logs` читается за окно:

- `logged_at >= date - 1 day`;
- `logged_at < date + 1 day`;
- `query_text != ''`.

Если `corrected_query_text` пустой, для join используется нормализованный `query_text`, иначе нормализованный
`corrected_query_text`. В Elasticsearch отправляется `result_query_text`, сгруппированный с массивом
`sku_group_id`.

Elasticsearch-запрос использует `size=3000`, `parallel_jobs=12`, `chunk_size=12`, `explain=true`, фильтр
`sku_group.id IN sku_group_ids`, multi-match по полям из `config.yaml` и raw field value factors для рейтингов/заказов.
`size=3000` - верхний предел на запрос, фактический объем дополнительно ограничивается фильтром по
`sku_group_ids`. Если production DSL отличается от текущего builder, менять нужно только `job/search.py`.

## Output columns

Основные поля: `query`, `sku_group_id`, `product_id`, `sku_group_title`, `sell_price`, сырые рейтинги/заказы,
вклады field value factors, `bms`, `total_score`, `sku_group_emb`, `analysis`.

BM25 хранится отдельными `ARRAY<DOUBLE>` колонками для каждого поля Elasticsearch-запроса:
`bm25_skus_title_synonym`, `bm25_skus_title`, `bm25_skus_discovery_filter_values_title_ru`,
`bm25_skus_discovery_filter_values_title_uz`, `bm25_skus_filter_values_title_ru`,
`bm25_skus_filter_values_title_uz`, `bm25_category_title_ru`, `bm25_category_title_uz`,
`bm25_category_full_title_ru`, `bm25_category_full_title_uz`, `bm25_product_title_ru`,
`bm25_product_title_uz`, `bm25_product_title_ru_synonym`, `bm25_product_title_uz_synonym`,
`bm25_full_category_name`.

`total_score` берется из корня Elasticsearch `_explanation.value`; `analysis` хранит тот же разобранный explain
в JSON.

## DQ

Автогенерируемый dbt DQ должен проверить `not_null` и уникальность по primary key
`date, query, sku_group_id`. Табличные проверки распределений BM25/field factors не добавлены: значения зависят от
Elasticsearch scoring и могут меняться вместе с индексом или production DSL.

## Рантайм

Airflow/Python + `pyiceberg`, не Spark. Чтение Trino выполняется через `trino_search`, чтение Elasticsearch -
через `joblib` threading backend на 12 jobs чанками по 12 query-групп, запись в Iceberg - через entity-local
`job/runtime.py`. Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, ресурсы pod: 12 CPU и
16Gi memory.

Воркеры только читают Elasticsearch и возвращают строки в parent task. Перед записью parent task очищает
партицию `date` одним PyIceberg `overwrite` пустым набором, затем последовательно append-ит чанки в эту
партицию. При retry DAG снова начинает с очистки партиции, поэтому partial append предыдущей попытки не
накапливается.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
