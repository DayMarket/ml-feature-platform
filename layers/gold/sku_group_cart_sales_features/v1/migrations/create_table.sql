CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    cart_sales_count_7d BIGINT COMMENT 'Количество distinct order_id продаж из CART за окно [ds - 6, ds]',
    cart_sales_count_14d BIGINT COMMENT 'Количество distinct order_id продаж из CART за окно [ds - 13, ds]',
    cart_sales_count_28d BIGINT COMMENT 'Количество distinct order_id продаж из CART за окно [ds - 27, ds]'
)
USING iceberg
COMMENT 'Gold-таблица признаков количества продаж из CART на уровне sku_group_id'
PARTITIONED BY (date)
