# Загрузка релевантности категории запросу

Публикует `iceberg.gold.feature_platform_query_category_relevance_expanded` в
ranking-service: набор `query_category_relevance` модели `search_unified_model_clusters`,
коллекция `SKU_GROUP_CATEGORY_TO_QUERY`.

Отдельный DAG со своим групповым тегом и алертами поиска; код job'а и
SparkApplication-шаблон переиспользуются из `upload/features_service_upload/v1`,
здесь только конфиг, DAG и фабрика.

## Оркестрация

- DAG: `feature-platform.upload.query_category_relevance_upload`.
- Групповой тег Airflow: `query-category-relevance` (общий с DAG'ом витрины).
- Расписание: `0 4 * * *` UTC, `start_date=2026-09-14T00:00:00+00:00`, `catchup=False`,
  DAG создаётся на паузе.
- Владелец `team:search`, алерты `search`, severity `P3`, webhook `oncall_webhook_search`.
- Сенсор: таска `dq` DAG'а
  `feature-platform.layers.gold.category_id_query_text.query_category_relevance_expanded`
  (delta `30` минут: `D 04:00 - 30 мин = D 03:30`).

## Чтение источника

`read_mode` не задан: читается партиция `date = {{ ds }}`. В ней уже полный снимок —
одна строка на пару `category_id, query_text` со всеми формулировками `query_id` в нижнем
регистре (логика — в README таблицы-источника). Каждый прогон заново отправляет все пары,
а не только изменившиеся.

Если партиции за день нет, job пишет в лог `rows=0` и пропускает запись в Kafka.

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

- `executor_instances: 4` — под ~27,6 млн сообщений за прогон (замер 2026-09-14),
  у основного upload'а 3. Перепроверить по длительности первого прогона.
