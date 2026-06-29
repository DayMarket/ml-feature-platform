CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'UTC-дата snapshot, соответствует data_interval_end расчета',
    product_id BIGINT COMMENT 'ID product из iceberg.silver.sku',
    search_queries ARRAY<STRUCT<search_query: STRING, uniq_installs: BIGINT>> COMMENT 'Поисковые запросы, в которых product_id встречался среди ranking candidates за последние 14 дней; отсортированы по uniq_installs по убыванию'
)
USING iceberg
COMMENT 'Silver-витрина поисковых запросов по product_id из ranking candidates за 14 дней'
PARTITIONED BY (date)
