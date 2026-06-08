CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIINT COMMENT 'ID sku group',
    home_reco_avg_sales_count_7d DOUBLE COMMENT 'Среднее дневное количество завершенных товарных позиций из рекомендаций с главной за окно [ds - 7, ds - 1]'
)
USING iceberg
COMMENT 'Gold-таблица среднего количества продаж из рекомендаций с главной за последние 7 дней на уровне sku_group_id'
PARTITIONED BY (date)
