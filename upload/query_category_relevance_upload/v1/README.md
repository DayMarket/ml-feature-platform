# Загрузка релевантности категории запросу

Публикует `iceberg.gold.feature_platform_query_category_relevance` в ranking-service:
набор `query_category_relevance` модели `search_unified_model_clusters`, коллекция
`SKU_GROUP_CATEGORY_TO_QUERY`.

Отдельный DAG, а не ещё одна группа в `upload/features_service_upload/v1/config.yaml`:
общий upload публикует одну партицию `{{ ds }}`, а эта группа — всю витрину
(`read_mode: full_table`). Код job'а и SparkApplication-шаблон переиспользуются из
`upload/features_service_upload/v1`; здесь только конфиг, DAG и фабрика.

## Оркестрация

- DAG: `feature-platform.upload.query_category_relevance_upload`.
- Групповой тег Airflow: `query-category-relevance` (общий с DAG'ом витрины).
- Расписание: `0 4 * * *` UTC, `start_date=2026-09-14T00:00:00+00:00`, `catchup=False`,
  DAG создаётся на паузе.
- Владелец `team:search`, алерты `search`, severity `P3`, webhook `oncall_webhook_search`.
- Сенсор: таска `materialize` DAG'а
  `feature-platform.layers.gold.category_id_query_text.query_category_relevance`
  (delta `60` минут: `D 04:00 - 1ч = D 03:00`).
  Это исключение из правила «upload ждёт `dq`»: у DAG'а витрины тасок `dq` нет по решению
  владельца, причина записана в `source.dq_waiver_reason`. Валидатор отклонит исключение,
  как только DAG витрины начнёт строить `dq`.

## Чтение источника

`read_mode: full_table` — фильтра по дате нет. Одна пара `category_id, query_text`
встречается в нескольких партициях, а порядок сообщений в Kafka при записи из Spark
не определён, поэтому job оставляет по каждой паре строку с самой свежей `date`
(`row_number() over (partition by category_id, query_text order by date desc) = 1`).
Каждый прогон заново отправляет все пары, а не только изменившиеся.

Пока витрина — заглушка и пуста, job пишет в лог `rows=0` и пропускает запись в Kafka.

## Kafka

- Connection: `kafka_ranking`, topic `ranking.features.updates`.
- Сообщение: `FeaturesUpdate.skuGroupCategoryToQueryFeatureSet`
  с `skuGroupCategoryId = category_id`, `query = query_text`, `features = [relevance]`.
  Сообщение есть в `ranking-python-client` начиная с 3.0.4; образ upload'а собран
  с 3.0.6, пересборка не нужна.
- Ключ сообщения: `query_category_relevance|<category_id>|<query_text>`.
- NULL в `relevance` отправляется как `0.0`, то есть так же, как отсутствие пары
  (`missing-feature-value: 0.0`).

## Требует проверки

- `executor_instances: 2` — стартовое значение для пустой витрины. Перепроверить
  по первому прогону на заполненной таблице: скан всей таблицы растёт с числом партиций.
