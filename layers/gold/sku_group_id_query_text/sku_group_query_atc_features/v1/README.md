# Gold-фичи ATC по Query и SKU Group

DAG id: `feature-platform.layers.gold.sku_group_id_query_text.sku_group_query_atc_features`.

Пайплайн строит дневные признаки конверсии из показа в добавление в корзину для пары `query_text` и `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_features`.

Основная логика:

- читает silver-таблицу `iceberg.silver.feature_platform_search_sku_group_id_install_query`;
- использует только записи со `space = 'SEARCH_RESULTS'`;
- нормализует текст запроса через `lower(trim(...))`;
- считает окна за 1, 3, 7, 14, 21, 30, 60 и 90 дней;
- формирует конверсии `query_skg_conv_imp2atc_*`;
- формирует доли `share_of_atc_*` внутри поискового запроса;
- пишет результат в Iceberg через `overwritePartitions()`.

Партиция результата соответствует Airflow `ds`.
