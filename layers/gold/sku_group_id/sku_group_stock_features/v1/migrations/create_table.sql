CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    skg_total_stock_1 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 1, ds - 1]',
    skg_total_stock_3 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 3, ds - 1]',
    skg_total_stock_7 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 7, ds - 1]',
    skg_total_stock_14 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 14, ds - 1]',
    skg_total_stock_21 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 21, ds - 1]',
    skg_total_stock_30 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 30, ds - 1]',
    skg_total_stock_60 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 60, ds - 1]',
    skg_total_stock_90 DOUBLE COMMENT 'Суммарный активный EOD-остаток sku_group_id за окно [ds - 90, ds - 1]'
)
USING iceberg
COMMENT 'Gold-фичи активного EOD-остатка по sku_group_id'
PARTITIONED BY (date)
