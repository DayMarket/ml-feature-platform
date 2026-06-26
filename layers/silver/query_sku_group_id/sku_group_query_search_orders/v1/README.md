# Silver-заказы из поиска по Query и SKU Group ID

DAG id: `feature-platform.layers.silver.query_sku_group_id.sku_group_query_search_orders`.

Пайплайн собирает дневную статистику заказов из поиска на уровне `query` и `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_sku_group_query_search_orders`.

`orders_generated` считается как количество уникальных `order_item_id` из поисковой атрибуции. Это сохранено намеренно для обратной совместимости gold-признаков `query_skg_uniq_orders_*` со старым feature-store подходом.

Основная логика:

- читает поисковую атрибуцию из `iceberg.silver.order_items_attribution`;
- читает заказы из `iceberg.silver.order_items`;
- обогащает заказы SKU-данными из `iceberg.silver.sku`;
- берет поисковые attribution-события за окно от `ds - 20 дней` до `next_ds`;
- считает generated, completed и returned метрики за расчетный день;
- пишет результат в Iceberg через `overwritePartitions()`.

Партиция результата соответствует Airflow `ds`.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/silver/query_sku_group_id/sku_group_query_search_orders/v1/entrypoints/get_sku_group_query_search_orders.py`, поэтому отдельный Docker image для этой сущности не собирается.
