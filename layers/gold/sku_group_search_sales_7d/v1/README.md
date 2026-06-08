# Gold Search Sales 7D по SKU Group ID

Пайплайн собирает дневной признак количества продаж из поиска на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_search_sales_7d`.

Источник:

- `iceberg.silver.feature_platform_sku_group_query_search_orders` - дневные поисковые заказы по `query` и `sku_group_id`.

Признак:

- `search_sales_count_7d` - сумма `items_completed` за окно `[ds - 7, ds - 1]`.

Для `ds = 2026-06-08` окно использует даты `[2026-06-01, 2026-06-07]`, сам `ds` в расчет не входит. `items_completed` в silver-источнике считается как количество завершенных товарных позиций из поисковой атрибуции, без возвратов до конца дневного окна.

Пайплайн пишет партицию `date = ds` в Iceberg через `overwritePartitions()`.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_search_sales_7d/v1/entrypoints/get_sku_group_search_sales_7d.py`, поэтому отдельный Docker image для этой сущности не собирается.
