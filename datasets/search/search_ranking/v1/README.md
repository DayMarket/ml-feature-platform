# Search ranking dataset v1

## Назначение

`datasets/search/search_ranking/v1` собирает обучающую выборку для search ranking на уровне поискового показа. Таблица предназначена только для offline training/evaluation и не публикуется в ranking-service или другой inference-сервис.

## Output

- Таблица: `iceberg.silver.feature_platform_dataset_search_ranking_v1`
- Путь в репозитории: `datasets/search/search_ranking/v1`
- Primary key: `collection_date, event_date, install_id, session_id, query, position`
- Партиция Iceberg: `collection_date`

## Orchestration

- DAG id: `feature-platform.datasets.search.search_ranking.v1`
- Group tag: `search-ranking-dataset`
- Schedule: `0 2 * * *` UTC
- Start date: `2026-06-09T00:00:00Z`
- Первая фактическая дата событий: `2026-05-20`
- Лаг сбора: `20` дней. Для логической даты DAG `D` таблица собирает `event_date = D - 20 days`.

## Источники

- `iceberg.silver_b2c_clickstream.events` - события `PRODUCT_IMPRESSION` из search results.
- `iceberg.silver.order_items_attribution` - атрибуция заказов к поисковой сессии и запросу.
- `iceberg.silver.order_items` - статусы и generated GMV товарных позиций заказа.
- `iceberg.silver.sku` - маппинг `sku_id -> sku_group_id`.
- `iceberg.silver.ranking_analytics_events` - ranking analytics logs со score-массивами по кандидатам.

Эти источники не объявлены как repository-managed tables в `layers/**/config.yaml` или `datasets/**/config.yaml`, поэтому DAG не ставит feature-platform DQ sensors. Upstream freshness/DQ контракт должен подтверждаться отдельно у владельцев источников.

## Логика сбора

Для каждой логической даты DAG `collection_date` рассчитывается `event_date = collection_date - 20 days`.

События показов берутся из `iceberg.silver_b2c_clickstream.events`:

- `received_at >= event_date`;
- `received_at < event_date + 1 day`;
- `logged_at >= event_date - 3 days`;
- `logged_at < event_date + 4 days`;
- `event_type = 'PRODUCT_IMPRESSION'`;
- `widget_space_name = 'SEARCH_RESULTS'`;
- `widget_section_name = 'SEARCH_RESULTS'`;
- `query IS NOT NULL`;
- `trim(query) != ''`;
- `COALESCE(is_full_catpred, false) = false`.

Запрос нормализуется как `trim(lower(query))`.

Перед join с заказами показы дедуплицируются по position key:

- `event_date`;
- `install_id`;
- `session_id`;
- `query`;
- `position`.

Из дублей сохраняется самый ранний показ по `received_at`, затем `logged_at`, затем минимальный `sku_group_id` для детерминизма. Это означает, что если в одном `event_date, install_id, session_id, query, position` встретились разные `sku_group_id`, в датасет попадет один выбранный кандидат. Заказный сигнал не влияет на выбор кандидата, потому что дедупликация выполняется до join с заказами. В колонке `position_duplicate_count` хранится количество сырых кандидатов этого position key.

После дедупликации добавляется `deduplicate_rank`: `row_number` внутри `event_date, install_id, session_id, query`, отсортированный по `position`, `received_at`, `logged_at`, `sku_group_id`. Эта колонка не удаляет строки и нужна, чтобы training code мог применять собственную политику отбора повторов внутри поискового контекста.

Заказы берутся из атрибуции:

- `event_received_at >= event_date`;
- `event_received_at < event_date + 1 day`;
- `query != ''`;
- `widget_space_name IN ('SHOP_SEARCH_RESULTS', 'COLLECTION_SEARCH_RESULTS', 'SEARCH', 'SEARCH_RESULTS')`;
- `COALESCE(is_full_catpred, 'false') = 'false'`.

`order_items` фильтруется:

- `order_item_status NOT IN ('CREATED', 'NOT_CREATED')`;
- `generated_at >= event_date - 15 days`;
- `generated_at < event_date + 1 day`.

Перед join с показами заказы агрегируются до `install_id, last_search_session_id, query, sku_group_id`, чтобы несколько `order_item_id` не размножали impression-строку. Агрегат хранит только binary label `is_generated_order = 1`; GMV, статусы и timestamps заказов в v1 не сохраняются.

Метка `is_generated_order` равна `1`, если для `install_id, session_id, query, sku_group_id` найден хотя бы один атрибутированный заказ, иначе `0`.

Score-поля берутся из `iceberg.silver.ranking_analytics_events` за тот же `event_date`:

- `fired_at >= event_date`;
- `fired_at < event_date + 1 day`;
- `model_name LIKE '%search_uni%'`;
- `search_query IS NOT NULL`;
- `trim(search_query) != ''`;
- `ranking_candidates` непустой;
- `external_features IS NOT NULL`.

`search_query` нормализуется как `trim(lower(search_query))`, чтобы join совпадал с нормализацией `query` в
показах. Из `external_features` читаются JSON-массивы `$.normalized_linear_score`, `$.linear_score` и
`$.dssm_score`. Массивы сопоставляются с `ranking_candidates` по позиции через `arrays_zip`, затем scores
усредняются до `query, sku_group_id` и `LEFT JOIN`-ятся к показам. Если score-массив отсутствует, не
парсится или не содержит значения для позиции кандидата, соответствующая score-колонка остается `NULL`.

## Output columns

- `collection_date` - логическая дата запуска DAG.
- `event_date` - дата событий, `collection_date - 20 days`.
- `logged_at`, `received_at` - времена исходного показа.
- `install_id`, `session_id`, `query`, `position` - ключевые поля позиции.
- `sku_group_id` - выбранная sku group для позиции после дедупликации.
- `deduplicate_rank` - порядковый номер показа внутри `event_date, install_id, session_id, query`.
- `position_duplicate_count` - количество сырых кандидатов position key до дедупликации.
- `widget_section_name`, `widget_space_name` - контекст виджета.
- `normalized_linear_score` - средний `normalized_linear_score` по `query, sku_group_id` из ranking analytics за `event_date`.
- `linear_score` - средний `linear_score` по `query, sku_group_id` из ranking analytics за `event_date`.
- `dssm_score` - средний `dssm_score` по `query, sku_group_id` из ranking analytics за `event_date`.
- `is_generated_order` - binary label.

## DQ

Автогенерируемый dbt DQ должен проверить `not_null` и уникальность по primary key. Табличные DQ проверки для распределения label, полноты партиций, допустимых значений `is_generated_order` или score-диапазонов не добавлены в этой версии, чтобы сначала накопить статистику по объему и стабильности источников.
