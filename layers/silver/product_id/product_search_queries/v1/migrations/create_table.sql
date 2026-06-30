CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'UTC-дата snapshot, соответствует data_interval_end расчета',
    product_id BIGINT COMMENT 'ID product из iceberg.silver.sku',
    search_queries ARRAY<STRING> COMMENT 'Top-200 поисковых запросов, в которых product_id встречался среди ranking candidates за последние 14 дней; отсортированы по числу уникальных install_id по убыванию'
)
USING iceberg
COMMENT 'Silver-витрина поисковых запросов по product_id из ranking candidates за 14 дней'
PARTITIONED BY (date)
