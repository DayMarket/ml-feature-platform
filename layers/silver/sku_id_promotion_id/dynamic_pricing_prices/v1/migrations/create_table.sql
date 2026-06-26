CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Timestamp расчета snapshot из data_interval_end в UTC',
    sku_id BIGINT COMMENT 'ID SKU из kazanexpress.public.sku',
    promotion_id STRING COMMENT 'ID promotion из config.yaml, по которому фильтруется dynamic_discount',
    sku_group_id BIGINT COMMENT 'ID sku group из kazanexpress.public.sku',
    product_id BIGINT COMMENT 'ID product из kazanexpress.public.sku',
    discount DOUBLE COMMENT 'Фактически примененная скидка; 0, если sell_price не совпал с calculated_for_price',
    sell_price DOUBLE COMMENT 'Итоговая цена продажи после фактически примененной скидки',
    discount_fraction DOUBLE COMMENT 'Доля скидки: discount / sell_price; NULL при нулевой итоговой цене',
    dynamic_discount_created_at TIMESTAMP COMMENT 'Timestamp последней записи dynamic_discount за последние 14 дней для SKU и promotion_id'
)
USING iceberg
COMMENT 'Silver snapshot цен SKU с учетом динамического ценообразования'
PARTITIONED BY (days(calculated_at))
