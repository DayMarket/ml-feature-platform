CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует dt источника sku_eod',
    sku_id BIGNT COMMENT 'ID SKU',
    total_stock BIGINT COMMENT 'Суммарный активный остаток SKU на конец дня: SUM(quantity_active_eod)'
)
USING iceberg
COMMENT 'Дневной silver-признак активного остатка на уровне sku_id'
PARTITIONED BY (date)
