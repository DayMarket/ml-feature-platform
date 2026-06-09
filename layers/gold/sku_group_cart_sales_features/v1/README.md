# Gold Cart Sales Features по SKU Group ID

Пайплайн собирает daily-признаки количества продаж из корзины на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_cart_sales_features`.

Источники:

- `iceberg.silver.order_items_attribution` - атрибуция товарной позиции, фильтр `widget_space_name = 'CART'`;
- `iceberg.silver.order_items` - заказные позиции, статусы и даты завершения;
- `iceberg.silver.sku` - маппинг `sku_id -> sku_group_id`.

Grain и ключ: одна строка на `date, sku_group_id`. Партиция - `date`.

Окна включают Airflow `{{ ds }}`. Так как `ds` в DAG соответствует вчерашнему бизнес-дню, для `ds = 2026-06-08` окно `7d` использует даты `[2026-06-02, 2026-06-08]`.

Признаки:

- `cart_sales_count_7d` - `COUNT(DISTINCT order_id)` за окно `[ds - 6, ds]`;
- `cart_sales_count_14d` - `COUNT(DISTINCT order_id)` за окно `[ds - 13, ds]`;
- `cart_sales_count_28d` - `COUNT(DISTINCT order_id)` за окно `[ds - 27, ds]`.

В расчет попадают только `order_items` со статусом `COMPLETED`, `issued_at` внутри 28-дневного окна и CART-атрибуцией по `order_item_id`. Позиции, возвращенные до конца расчетного дня, исключаются через условие `returned_at IS NULL OR returned_at >= {{ next_ds }}`.

Для поиска CART-атрибуции используется дополнительный 20-дневный lookback от начала 28-дневного окна, как в существующих order-пайплайнах репозитория.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_cart_sales_features/v1/entrypoints/get_sku_group_cart_sales_features.py`, поэтому отдельный Docker image для этой сущности не собирается.
