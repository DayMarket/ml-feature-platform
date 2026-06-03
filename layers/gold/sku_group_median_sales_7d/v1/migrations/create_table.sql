CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата расчета признака',
    sku_group_id BIGINT COMMENT 'Идентификатор SKU group',
    median_sales_count_7d DOUBLE COMMENT 'Медианное суточное количество завершенных продаж за последние 7 суток'
)
USING iceberg
COMMENT 'Gold-таблица медианных продаж за последние 7 суток на уровне sku_group_id'
PARTITIONED BY (date)
