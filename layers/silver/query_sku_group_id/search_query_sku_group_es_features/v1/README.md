# Search Query/SKU Group Elasticsearch Features

Пайплайн собирает silver-признаки из Elasticsearch `_explanation` на уровне поискового запроса и `sku_group_id`.
Сбор raw-ответов Elasticsearch вынесен в отдельный DAG; основной writer DAG готовит parquet на S3 и загружает
дневную партицию в Iceberg.

## Выход и оркестрация

- Финальная таблица: `iceberg.silver.feature_platform_search_query_sku_group_es_features`.
- Prepared S3 path: `airflow/2026/bm25_features/prepared/date=<YYYY-MM-DD>/run_id=<run_id>/`.
- Elasticsearch collect DAG: `feature-platform.layers.silver.query_sku_group_id.search_query_sku_group_es_features.elasticsearch_collect`.
- Writer DAG: `feature-platform.layers.silver.query_sku_group_id.search_query_sku_group_es_features`.
- Путь: `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1`.
- Групповой тег Airflow: `search-es-features`.
- Elasticsearch collect DAG расписание: ежедневно в 04:00 UTC, `0 4 * * *`.
- Writer DAG: trigger-only, запускается collect DAG-ом после записи raw JSONL в S3, пишет prepared parquet на S3
  и загружает дневную партицию в Iceberg.
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
- `logged_at >= date` и `logged_at < date + 1 day`.

`search_logs` читается за окно:

- `logged_at >= date - 1 day`;
- `logged_at < date + 1 day`;
- `query_text != ''`.

Если `corrected_query_text` пустой, для join используется нормализованный `query_text`, иначе нормализованный
`corrected_query_text`. В Elasticsearch отправляется `result_query_text`, сгруппированный с массивом
`sku_group_id`, только если `COUNT(DISTINCT install_id) >= 2` за окно `search_logs`. Порог задается в config как
`source.min_result_query_installs`.

Elasticsearch-запрос использует `size=3000`, `parallel_jobs=24`, `chunk_size=3000`, `write_chunk_size=50000`,
`explain=true`, фильтр
`sku_group.id IN sku_group_ids`, multi-match по полям из `config.yaml` и raw field value factors для рейтингов/заказов.
`size=3000` - верхний предел на запрос, фактический объем дополнительно ограничивается фильтром по
`sku_group_ids`. Если production DSL отличается от текущего builder, менять нужно только `job/search.py`.

Elasticsearch collect DAG пишет ответы Elasticsearch в `jsonl.gz`:

`airflow/2026/bm25_features/raw/date=<YYYY-MM-DD>/run_id=<run_id>/chunk=<NNNNNN>/part-<NNNNNN>.jsonl.gz`.

Одна строка raw-файла содержит `date`, `query` и Elasticsearch `hit` с `_source` и `_explanation`. После записи raw
collect DAG триггерит writer DAG с `partition_date`. Writer DAG сначала ждет успешный Elasticsearch collect DAG через
`ExternalTaskSensor`, затем читает date-level `manifest.json`; если manifest отсутствует, он выбирает последний `run_id`
за дату и строит список входов из `chunk=*/part-*.jsonl.gz`. После этого writer парсит raw hits существующим
`job/analyze.py` и пишет подготовленные parquet-файлы:

`airflow/2026/bm25_features/prepared/date=<YYYY-MM-DD>/run_id=<run_id>/chunk=<NNNNNN>/part-<NNNNNN>.parquet`.

Для prepared-слоя дополнительно публикуются `run_id=<run_id>/manifest.json`, date-level `manifest.json` и `_SUCCESS`.
После этого Iceberg-load task ждет 20 секунд, читает prepared parquet из manifest или по списку
`prepared/date=<YYYY-MM-DD>/run_id=*/chunk=*/part-*.parquet`, выбирает последний `run_id`, stage-ит delete партиции
`date=<YYYY-MM-DD>` и append всех parquet-файлов в одной Iceberg transaction, затем делает один commit.

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
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, ресурсы pod: 16 CPU и 32Gi memory.
Raw S3 запись использует `boto3`; connection `search_research_bucket` должен содержать bucket в `host`,
`extra.bucket` или `extra.bucket_name`, а также endpoint/credentials для S3-compatible storage.

Elasticsearch collect DAG читает Trino через `trino_search`, Elasticsearch - через `joblib` threading backend на 24 jobs чанками по
3000 query-групп. Ответы Elasticsearch читаются streaming generator-ом и сохраняются в `jsonl.gz` частями по
`raw_storage.file_row_limit=50000` строк; полный список строк ES chunk-а не держится в памяти.

Writer DAG читает raw `jsonl.gz` из manifest или списка `chunk=*/part-*.jsonl.gz`, парсит hits и пишет parquet-файлы
блоками по `write_chunk_size=50000`. Затем `load_to_iceberg` читает prepared parquet по одному part-файлу, не собирая
все 381 parquet в один DataFrame, и загружает их в Iceberg через один metadata commit. Retry writer DAG-а очищает
prepared-prefix текущего `run_id`, перечитывает уже сохраненный raw без повторного похода в Elasticsearch, а финальная
Iceberg transaction заново перезаписывает только партицию `date`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
