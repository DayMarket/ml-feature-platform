# Gold Aggregated Conversions Query/SKU Group Legacy

DAG id: `feature-platform.layers.gold.query_sku_group_id.query_skg_aggregated_conversions_legacy`.

Пайплайн собирает legacy-агрегации query/SKU group conversions из дневной silver-таблицы.

Целевая таблица: `iceberg.gold.feature_platform_query_skg_aggregated_conversions_legacy`.

Источник: `iceberg.silver.feature_platform_query_skg_daily_conversions_legacy`.

Зерно: `date`, `query`, `sku_group_id`.

Основная логика:

- читает дневные метрики за период от `{{ ds }} - 90 days` до `{{ ds }}` включительно;
- сначала суммирует дневные метрики по `date`, `query`, `sku_group_id`, тем самым схлопывая `platform`;
- строит окна `1`, `3`, `7`, `14`, `21`, `30`, `60`, `90` дней;
- граница каждого окна повторяет старую формулу: `date >= {{ ds }} - N days`, `date <= {{ ds }}`;
- считает `query_skg_uniq_impressions_*`, `query_skg_uniq_clicks_*`, `query_skg_uniq_atcs_*`, `query_skg_uniq_orders_*`;
- считает `query_skg_conv_imp2click_*`, `query_skg_conv_imp2atc_*`, `query_skg_conv_imp2order_*` через обычное Spark-деление;
- не заменяет `NULL` деления на `0.0`;
- применяет legacy-фильтр `query_skg_uniq_impressions_14 >= 2`;
- считает ratio-признаки для пар окон `3/1`, `7/3`, `14/7`, `30/14`, `60/30`, `90/60`.

Таблица является источником для `iceberg.gold.feature_platform_query_skg_pairwise_features_legacy`.

