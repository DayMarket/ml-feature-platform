# Gold-фичи ATC и заказов по Query и SKU Group

Пайплайн строит дневные признаки конверсий и отношений окон на уровне пары `query` и `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features`.

Основная логика:

- читает поисковые события из `iceberg.silver.feature_platform_search_sku_group_id_install_query`;
- читает поисковые заказы из `iceberg.silver.feature_platform_sku_group_query_search_orders`;
- использует окно до 90 дней от расчетной даты Airflow `ds`;
- агрегирует ATC, показы и сгенерированные заказы по окнам 1, 3, 7, 14, 21, 30, 60 и 90 дней;
- считает конверсии `impression -> atc` и `impression -> order`;
- считает отношения конверсий между соседними окнами;
- оставляет пары с `query_skg_uniq_impressions_14 >= 2`;
- пишет результат в Iceberg через `overwritePartitions()`.

DAG ждет silver DAG-и `feature_platform_sku_group_install_silver_stats_dag` и `feature_platform_sku_group_query_search_orders_silver_dag`.
