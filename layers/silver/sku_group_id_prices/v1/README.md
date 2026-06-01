# Silver-статистика SKU Group по Install

Пайплайн собирает дневную предагрегированную статистику поисковых и категорийных взаимодействий на уровне `install_id`, `sku_group_id` и текста запроса или категории.

Целевая таблица: `iceberg.silver.feature_platform_search_sku_group_id_install_query`.

Основная логика:

- читает события из `iceberg.silver_b2c_clickstream.events` за партиционный интервал;
- использует `iceberg.silver.sku` для fallback-маппинга товара в `sku_group_id`;
- учитывает `SEARCH_RESULTS`, `PRODUCT_IMPRESSION`, `PRODUCT_VIEW` и `ADD_TO_CART`;
- считает показы, клики и добавления в корзину;
- пишет результат в Iceberg через `overwritePartitions()`.

Партиция расчета задается аргументами Airflow `partition_start` и `partition_end`.
