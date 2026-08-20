CREATE TABLE IF NOT EXISTS {target_table} (
    snapshot_date DATE COMMENT 'Дата входного снимка SKU в UTC',
    sku_id BIGINT COMMENT 'Идентификатор SKU',
    product_id BIGINT COMMENT 'Идентификатор товара для финальной агрегации',
    dimensional_group STRING COMMENT 'Габаритная группа SKU: SMALL, MEDIUM или LARGE',
    sell_price_uzs DOUBLE COMMENT 'Историческая sell price SKU в UZS за snapshot_date',
    commission_pct DOUBLE COMMENT 'Фактическая комиссия SKU в процентах либо NULL',
    n_orders_28d BIGINT COMMENT 'Количество строк заказов SKU за предшествующие 28 дней'
)
USING iceberg
COMMENT 'Silver: дневной SKU-вход для независимых CM2-расчётов'
PARTITIONED BY (snapshot_date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
