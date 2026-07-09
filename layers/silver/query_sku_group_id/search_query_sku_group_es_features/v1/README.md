# Search Query/SKU Group Elasticsearch Features

Пайплайн собирает silver-признаки из Elasticsearch `_explanation` на уровне поискового запроса и `sku_group_id`.
Сбор raw-ответов Elasticsearch и запись финальной Iceberg-таблицы разнесены на два DAG-а.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_search_query_sku_group_es_features`.
- Elasticsearch collect DAG: `feature-platform.layers.silver.query_sku_group_id.search_query_sku_group_es_features.elasticsearch_collect`.
- Writer DAG: `feature-platform.layers.silver.query_sku_group_id.search_query_sku_group_es_features`.
- Путь: `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1`.
- Групповой тег Airflow: `search-es-features`.
- Elasticsearch collect DAG расписание: ежедневно в 04:00 UTC, `0 4 * * *`.
- Writer DAG: trigger-only, запускается collect DAG-ом после успешной записи manifest в S3.
- Writer DAG содержит `ExternalTaskSensor` на Elasticsearch collect DAG за тот же `partition_date`.
- При ручном запуске writer DAG можно передать `{"partition_date": "YYYY-MM-DD"}`; если conf не передан,
  используется предыдущий UTC-день от logical date запуска.
- `start_date=2026-03-13T00:00:00Z`, `catchup=False`.

## Грейн / ключ

`date, query, sku_group_id`.

`date` - закрытый UTC-день, равный `data_interval_end - 1 day`. Эта дата используется как дата clickstream
событий и как `log_date` результата.

## Источники

- `iceberg.silver_b2c_clickstream.events` - события `PRODUCT_IMPRESSION` из search results, из которых берутся
  пары `query, sku_group_id`; отдельный sensor не ставится.
- `"dwh-iceberg".silver.search_logs` - внешний Trino-источник для `result_query_text`; отдельный sensor не
  ставится по подтвержденному контракту.
- Elasticsearch endpoint из Airflow connection `elasticsearch_search`, path `/_search`.
- Raw storage - Airflow connection `search_research_bucket`, prefix `airflow/2026/bm25_features`.

## Логика

Trino-шаг выбирает все возможные пары `query, sku_group_id` из clickstream за `received_at`-день `date`,
нормализуя запрос как `lower(trim(replace(query, 'ё', 'е')))`. Используются только события:

- `event_type = 'PRODUCT_IMPRESSION'`;
- `widget_space_name = 'SEARCH_RESULTS'`;
- `widget_section_name = 'SEARCH_RESULTS'`;
- `COALESCE(is_full_catpred, false) = false`;
- `received_at >= date` и `received_at < date + 1 day`;
- защитное окно по `logged_at`: `date - 3 days <= logged_at < date + 4 days`.

`search_logs` читается за окно:

- `logged_at >= date - 1 day`;
- `logged_at < date + 1 day`;
- `query_text != ''`.

Если `corrected_query_text` пустой, для join используется нормализованный `query_text`, иначе нормализованный
`corrected_query_text`. В Elasticsearch отправляется `result_query_text`, сгруппированный с массивом
`sku_group_id`.

Elasticsearch-запрос использует `size=3000`, `parallel_jobs=24`, `chunk_size=3000`, `write_chunk_size=50000`,
`explain=true`, фильтр
`sku_group.id IN sku_group_ids`, multi-match по полям из `config.yaml` и raw field value factors для рейтингов/заказов.
`size=3000` - верхний предел на запрос, фактический объем дополнительно ограничивается фильтром по
`sku_group_ids`. Если production DSL отличается от текущего builder, менять нужно только `job/search.py`.

Elasticsearch collect DAG пишет ответы Elasticsearch в `jsonl.gz`:

`airflow/2026/bm25_features/raw/date=<YYYY-MM-DD>/run_id=<run_id>/chunk=<NNNNNN>/part-<NNNNNN>.jsonl.gz`.

Одна строка raw-файла содержит `date`, `query` и Elasticsearch `hit` с `_source` и `_explanation`. После успешной
записи collect DAG публикует date-level `manifest.json` и `_SUCCESS`, затем триггерит writer DAG с `partition_date`.
Writer DAG сначала ждет успешный Elasticsearch collect DAG через `ExternalTaskSensor`, затем читает manifest, парсит raw hits
существующим `job/analyze.py` и пишет финальные колонки в Iceberg.

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

Airflow/Python + `pyiceberg`, не Spark. Образ задач:
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, ресурсы pod: 10 CPU и 16Gi memory.
Raw S3 запись использует `boto3`; connection `search_research_bucket` должен содержать bucket в `host`,
`extra.bucket` или `extra.bucket_name`, а также endpoint/credentials для S3-compatible storage.

Elasticsearch collect DAG читает Trino через `trino_search`, Elasticsearch - через `joblib` threading backend на 24 jobs чанками по
3000 query-групп. Ответы Elasticsearch читаются streaming generator-ом и сохраняются в `jsonl.gz` частями по
`raw_storage.file_row_limit=50000` строк; полный список строк ES chunk-а не держится в памяти.

Writer DAG читает raw `jsonl.gz` из manifest, удаляет партицию `date` в Iceberg отдельным commit-ом, затем парсит
hits и append-ит финальные строки блоками по `write_chunk_size=50000`. Каждый append делает отдельный Iceberg commit,
поэтому строки становятся видимыми по мере работы writer DAG-а, а retry writer DAG-а начинает с повторной очистки
партиции и перечитывает уже сохраненный raw без повторного похода в Elasticsearch.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
