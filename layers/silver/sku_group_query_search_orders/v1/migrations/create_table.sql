CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    query STRING COMMENT 'Текст поискового запроса',
    sku_group_id BIGINT COMMENT 'ID sku group',
    orders_generated BIGINT COMMENT 'Количество уникальных order_item_id, сгенерированных из поиска',
    items_generated BIGINT COMMENT 'Количество сгенерированных товарных позиций из поиска',
    gmv_generated DOUBLE COMMENT 'GMV сгенерированных заказов из поиска',
    items_completed BIGINT COMMENT 'Количество завершенных товарных позиций из поиска',
    gmv_completed DOUBLE COMMENT 'GMV завершенных заказов из поиска',
    completed_orders BIGINT COMMENT 'Количество завершенных заказов из поиска',
    returned_items BIGINT COMMENT 'Количество возвращенных товарных позиций из поиска',
    returned_gmv DOUBLE COMMENT 'GMV возвращенных заказов из поиска',
    returned_orders BIGINT COMMENT 'Количество возвращенных заказов из поиска'
)
USING iceberg
COMMENT 'Дневная silver-статистика поисковых заказов на уровне query и sku_group_id'
PARTITIONED BY (date)
