CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Закрытые UTC-сутки, за которые собран агрегат; партиция',
    sku_id BIGINT COMMENT 'ID SKU из pricing.dynamic_discount',
    seller_price BIGINT COMMENT 'Самая частая calculated_for_price за сутки по всем плечам; у ~3% SKU цена продавца меняется внутри дня, поэтому берется мода, а не произвольное значение',
    dp_sell_price BIGINT COMMENT 'Самая частая итоговая цена calculated_for_price - discount_amount за сутки по всем promotion_id; только дино-скидка, скидки по карте не учтены',
    prices ARRAY<BIGINT> COMMENT 'Уникальные итоговые цены за сутки по всем плечам, отсортированы по возрастанию',
    observations INT COMMENT 'Число наблюдений плечо x батч, из которых посчитаны моды; нужен для оценки надежности агрегата'
)
USING iceberg
COMMENT 'Silver: дневной агрегат цен SKU по данным динамического ценообразования. Источник - ClickHouse pricing.dynamic_discount, содержащий ТОЛЬКО дино-скидки; карточные скидки в него не входят и в итоговую цену не заложены. Агрегация идет по всем promotion_id за сутки, а это A/B-плечи дино-модели, поэтому мода дает плечам равный вес независимо от распределения трафика. Ключ date + sku_id'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
