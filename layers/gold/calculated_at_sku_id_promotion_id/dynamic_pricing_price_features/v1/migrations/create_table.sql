CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Timestamp расчета snapshot из data_interval_end в UTC',
    sku_id BIGINT COMMENT 'ID SKU из kazanexpress.public.sku',
    promotion_id STRING COMMENT 'ID promotion из config.yaml',
    sku_group_id BIGINT COMMENT 'ID sku group из kazanexpress.public.sku',
    product_id BIGINT COMMENT 'ID product из kazanexpress.public.sku',
    calculated_for_price DOUBLE COMMENT 'Цена, для которой был рассчитан discount_amount',
    discount DOUBLE COMMENT 'Фактически примененная скидка; 0, если sell_price не совпал с calculated_for_price',
    sell_price DOUBLE COMMENT 'Итоговая цена продажи после фактически примененной скидки',
    discount_fraction DOUBLE COMMENT 'Доля скидки: discount / sell_price; NULL при нулевой итоговой цене',
    dynamic_discount_created_at TIMESTAMP COMMENT 'Timestamp последней dynamic_discount записи, использованной в расчете'
)
USING iceberg
COMMENT 'Gold snapshot финальных цен SKU с учетом динамического ценообразования'
PARTITIONED BY (days(calculated_at))
