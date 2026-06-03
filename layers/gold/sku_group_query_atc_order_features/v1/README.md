# Gold-фичи ATC и заказов по Query и SKU Group

Пайплайн строит дневные признаки конверсий и отношений окон на уровне пары `query` и `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features`.

Основная логика:

- читает поисковые события из `iceberg.silver.feature_platform_search_sku_group_id_install_query`;
- читает поисковые заказы из `iceberg.silver.feature_platform_sku_group_query_search_orders`;
- нормализует `query` в legacy-совместимом стиле: `lower`, замена `ё` на `е`, схлопывание пробелов, `trim`, фильтр пустых строк, токенизация, удаление stopwords, dedup и сортировка токенов в `base_query`;
- использует окно до 90 дней от расчетной даты Airflow `ds`;
- агрегирует ATC, показы и сгенерированные заказы по окнам 1, 3, 7, 14, 21, 30, 60 и 90 дней;
- считает конверсии `impression -> atc` и `impression -> order`;
- считает отношения конверсий между соседними окнами;
- для совместимости с legacy pairwise table делает carry-forward: объединяет текущий расчет с предыдущими партициями целевой gold-таблицы за 90 дней и оставляет последнюю доступную запись по `(query, sku_group_id)`;
- пишет результат в Iceberg через `overwritePartitions()`.

`query_skg_uniq_orders_*` строятся из `orders_generated`, который на silver-слое считается как количество уникальных `order_item_id` в новой feature_platform-атрибуции.

Деление в conversion и ratio признаках оставляет Spark-семантику `NULL` для нулевого или отсутствующего знаменателя. Это сделано для приближения к старому feature-store поведению, где такие значения не заменялись на `0.0` на этапе расчета pairwise-признаков.

DAG ждет DQ DAG-и silver-источников:

- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`;
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq`.

Пайплайн использует новый способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_query_atc_order_features/v1/entrypoints/get_sku_group_query_atc_order_features.py`, поэтому отдельный Docker image для этой сущности не собирается.
