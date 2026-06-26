# Silver Daily Conversions Query/SKU Group Legacy

DAG id: `feature-platform.layers.silver.query_platform_sku_group_id.query_skg_daily_conversions_legacy`.

Пайплайн собирает дневные legacy-конверсии поиска на уровне `date`, `query`, `platform`, `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_query_skg_daily_conversions_legacy`.

Основная логика:

- читает события из `iceberg.silver_b2c_clickstream.events` за UTC-интервал Airflow `data_interval_start` - `data_interval_end`;
- использует `iceberg.silver.sku` для fallback-маппинга `sku_id` в `sku_group_id`;
- повторяет старую query-нормализацию для событий: `lower(query)`, а затем `trim(query)` после джойна заказов;
- считает `uniq_impressions` как `count(distinct session_id)` для `PRODUCT_IMPRESSION` в `SEARCH_RESULTS`;
- считает `uniq_clicks` и `uniq_atcs` как `count(distinct session_id)` для `PRODUCT_VIEW`/`ADD_TO_CART`, если предыдущее событие в окне `session_id, sku_group_id` было из `SEARCH_RESULTS`;
- считает `uniq_orders` как `count(distinct order_item_id)` по текущим Iceberg-таблицам `iceberg.silver.order_items_attribution` и `iceberg.silver.order_items`;
- для заказов использует `widget_section_name = 'SEARCH_RESULTS'` и `last_atc_platform`, как в старом подходе;
- фильтрует пустой query, query `0`, строки без показов и строки без `sku_group_id`/`platform`.

Таблица является источником для gold-агрегации `iceberg.gold.feature_platform_query_skg_aggregated_conversions_legacy`.
