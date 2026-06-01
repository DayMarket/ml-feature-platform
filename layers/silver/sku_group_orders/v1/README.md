# Silver-заказы по SKU Group ID

Пайплайн собирает дневную статистику заказов на уровне `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_sku_group_orders`.

Основная логика:

- читает заказы из `iceberg.silver.order_items`;
- обогащает заказы SKU-данными из `iceberg.silver.sku`;
- использует окно от `ds - 20 дней` до `next_ds` для актуальных заказов;
- считает generated, completed и returned метрики за расчетный день;
- пишет результат в Iceberg через `overwritePartitions()`.

Партиция результата соответствует Airflow `ds`.
