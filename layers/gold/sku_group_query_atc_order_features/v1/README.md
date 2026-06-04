# Gold-фичи ATC и заказов по Query и SKU Group

Пайплайн строит дневные признаки конверсий и отношений окон на уровне пары `query` и `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features`.

Основная логика:

- читает поисковые события из `iceberg.silver.feature_platform_search_sku_group_id_install_query`;
- читает поисковые заказы из `iceberg.silver.feature_platform_sku_group_query_search_orders`;
- нормализует исходный `query`: `lower`, замена `ё` на `е`, схлопывание пробелов, `trim` и фильтр пустых строк;
- не преобразует запрос в `base_query`: не удаляет stopwords, не удаляет повторяющиеся токены и не меняет порядок слов;
- использует окно до 90 дней от расчетной даты Airflow `ds`;
- агрегирует ATC, показы и сгенерированные заказы по окнам 1, 3, 7, 14, 21, 30, 60 и 90 дней;
- использует пары из поисковых показов как основу набора ключей и присоединяет заказы через `LEFT JOIN`;
- оставляет только пары с `query_skg_uniq_impressions_14 >= 2`, как в legacy pairwise-расчете;
- исключает пары без единого ATC и заказа за 90 дней, поскольку все финальные признаки для них равны нулю;
- считает конверсии `impression -> atc` и `impression -> order`;
- считает отношения конверсий между соседними окнами;
- пишет результат в Iceberg через `overwritePartitions()`.

Gold-витрина не читает собственные предыдущие партиции для carry-forward. Актуальный дневной срез каждый раз формируется из silver-источников, чтобы старые пары не переносились в новые даты бесконечно.

`query_skg_uniq_orders_*` строятся из `orders_generated`, который на silver-слое считается как количество уникальных `order_item_id` в новой feature_platform-атрибуции.

Деление в conversion и ratio признаках оставляет Spark-семантику `NULL` для нулевого или отсутствующего знаменателя. Это сделано для приближения к старому feature-store поведению, где такие значения не заменялись на `0.0` на этапе расчета pairwise-признаков.

DAG ждет DQ DAG-и silver-источников:

- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`;
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq`.

Пайплайн использует новый способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_query_atc_order_features/v1/entrypoints/get_sku_group_query_atc_order_features.py`, поэтому отдельный Docker image для этой сущности не собирается.
