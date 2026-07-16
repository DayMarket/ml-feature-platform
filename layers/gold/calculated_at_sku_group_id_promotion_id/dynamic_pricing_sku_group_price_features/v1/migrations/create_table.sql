CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Timestamp расчета snapshot из data_interval_end в UTC',
    sku_group_id BIGINT COMMENT 'ID SKU group',
    promotion_id STRING COMMENT 'ID promotion из SKU-level витрины, включая дефолтный baseline promotion_id 0',
    min_sell_price DOUBLE COMMENT 'Минимальная итоговая цена SKU внутри sku_group_id/promotion_id',
    max_sell_price DOUBLE COMMENT 'Максимальная итоговая цена SKU внутри sku_group_id/promotion_id',
    avg_sell_price DOUBLE COMMENT 'Средняя итоговая цена SKU внутри sku_group_id/promotion_id',
    min_discount DOUBLE COMMENT 'Минимальная скидка SKU внутри sku_group_id/promotion_id',
    max_discount DOUBLE COMMENT 'Максимальная скидка SKU внутри sku_group_id/promotion_id',
    avg_discount DOUBLE COMMENT 'Средняя скидка SKU внутри sku_group_id/promotion_id',
    min_discount_fraction DOUBLE COMMENT 'Минимальная доля скидки SKU внутри sku_group_id/promotion_id',
    max_discount_fraction DOUBLE COMMENT 'Максимальная доля скидки SKU внутри sku_group_id/promotion_id',
    avg_discount_fraction DOUBLE COMMENT 'Средняя доля скидки SKU внутри sku_group_id/promotion_id'
)
USING iceberg
COMMENT 'Gold агрегаты dynamic-pricing цен и скидок на уровне sku_group_id/promotion_id'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
