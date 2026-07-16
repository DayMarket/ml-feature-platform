CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата закрытого UTC-дня, за который выбран latest dynamic_discount',
    sku_id BIGINT COMMENT 'ID SKU из promotions.public.dynamic_discount',
    promotion_id STRING COMMENT 'ID promotion из config.yaml, по которому фильтруется dynamic_discount',
    discount_amount DOUBLE COMMENT 'Размер скидки из последней записи dynamic_discount за date',
    calculated_for_price DOUBLE COMMENT 'Цена, для которой был рассчитан discount_amount',
    created_at TIMESTAMP COMMENT 'Timestamp последней записи dynamic_discount за date для SKU и promotion_id'
)
USING iceberg
COMMENT 'Silver: дневной latest dynamic_discount на уровне SKU и promotion'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
