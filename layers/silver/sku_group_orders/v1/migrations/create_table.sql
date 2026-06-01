CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    orders_generated BIGINT COMMENT 'Количество сгенерированных заказов',
    items_generated BIGINT COMMENT 'Количество сгенерированных товарных позиций',
    gmv_generated DOUBLE COMMENT 'GMV сгенерированных заказов',
    items_completed BIGINT COMMENT 'Количество завершенных товарных позиций',
    gmv_completed DOUBLE COMMENT 'GMV завершенных заказов',
    completed_orders BIGINT COMMENT 'Количество завершенных заказов',
    returned_items BIGINT COMMENT 'Количество возвращенных товарных позиций',
    returned_gmv DOUBLE COMMENT 'GMV возвращенных заказов',
    returned_orders BIGINT COMMENT 'Количество возвращенных заказов'
)
USING iceberg
COMMENT 'Дневная silver-статистика заказов на уровне sku_group_id'
PARTITIONED BY (date)
