# Silver-заказы из поиска по Query и SKU Group ID

Пайплайн собирает дневную статистику заказов из поиска на уровне `query` и `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_sku_group_query_search_orders`.

Основная логика:

- читает поисковую атрибуцию из `iceberg.silver.order_items_attribution`;
- читает заказы из `iceberg.silver.order_items`;
- обогащает заказы SKU-данными из `iceberg.silver.sku`;
- берет поисковые attribution-события за окно от `ds - 20 дней` до `next_ds`;
- считает generated, completed и returned метрики за расчетный день;
- пишет результат в Iceberg через `overwritePartitions()`.

Партиция результата соответствует Airflow `ds`.
