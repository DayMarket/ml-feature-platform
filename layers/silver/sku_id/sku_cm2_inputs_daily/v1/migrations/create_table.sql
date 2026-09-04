CREATE TABLE IF NOT EXISTS {target_table} (
    dt TIMESTAMP COMMENT 'Начало даты выполнения расчёта (00:00:00 Asia/Tashkent); часть уникального ключа dt, sku_id',
    sku_id BIGINT COMMENT 'Идентификатор SKU; часть уникального ключа dt, sku_id',
    product_id INT COMMENT 'Идентификатор товара, по которому Main CM2 и PDP CM2 агрегируют SKU-значения в Gold',
    dimensional_group STRING COMMENT 'Габаритная группа SKU: SMALL, MEDIUM или LARGE; NULL, пустая строка и строка из пробелов в источнике заменяются на SMALL',
    sell_price_uzs DOUBLE COMMENT 'Историческая EOD sell price SKU в UZS за календарный день перед dt либо NULL',
    commission_pct DOUBLE COMMENT 'Фактическая комиссия SKU в процентах в диапазоне [0, 100] либо NULL',
    n_orders_28d BIGINT COMMENT 'Количество строк заказов SKU за 28 дней до начала даты расчёта dt в Asia/Tashkent; SKU без заказов получает 0'
)
USING iceberg
COMMENT 'Silver: дневной SKU-вход для независимых CM2-расчётов'
PARTITIONED BY (days(dt))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
