CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    search_sales_count_7d DOUBLE COMMENT 'Количество завершенных товарных позиций из поиска за окно [ds - 7, ds - 1]'
)
USING iceberg
COMMENT 'Gold-таблица количества продаж из поиска за последние 7 дней на уровне sku_group_id'
PARTITIONED BY (date)
