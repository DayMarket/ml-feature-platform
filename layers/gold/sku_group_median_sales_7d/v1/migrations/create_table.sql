CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует дате окончания трехчасового расчетного интервала',
    sku_group_id BIGINT COMMENT 'ID sku group',
    median_sales_count_7d DOUBLE COMMENT 'Медианное дневное количество проданных товаров за последние 7 суток'
)
USING iceberg
COMMENT 'Gold-фича медианного количества продаж за 7 суток на уровне sku_group_id'
PARTITIONED BY (date)
