CREATE TABLE IF NOT EXISTS {target_table} (
    dt DATE COMMENT 'Предыдущая полная календарная дата в Asia/Tashkent; часть уникального ключа dt, sku_id',
    sku_id INT COMMENT 'Идентификатор SKU; часть уникального ключа dt, sku_id',
    product_id INT COMMENT 'Идентификатор товара, по которому Main CM2 и PDP CM2 агрегируют SKU-значения в Gold',
    dimensional_group STRING COMMENT 'Габаритная группа SKU: SMALL, MEDIUM или LARGE; NULL источника заменяется на SMALL',
    sell_price_uzs DOUBLE COMMENT 'Историческая sell price SKU в UZS на дату dt либо NULL',
    commission_pct DOUBLE COMMENT 'Фактическая комиссия SKU в процентах в диапазоне [0, 100] либо NULL',
    n_orders_28d BIGINT COMMENT 'Количество строк заказов SKU за 28 полных дней до конца dt в Asia/Tashkent; SKU без заказов получает 0'
)
USING iceberg
COMMENT 'Silver: дневной SKU-вход для независимых CM2-расчётов'
PARTITIONED BY (dt)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
