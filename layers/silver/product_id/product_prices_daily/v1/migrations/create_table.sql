CREATE TABLE IF NOT EXISTS {target_table} (
    price_date DATE COMMENT 'Дата исторического EOD-среза цен в часовом поясе Asia/Tashkent',
    product_id INT COMMENT 'Идентификатор товара',
    min_sell_price_eod DOUBLE COMMENT 'Минимальная валидная sell price товара на конец дня после двухэтапной агрегации через SKU-group',
    avg_sell_price_eod DOUBLE COMMENT 'Среднее sell price по SKU-group товара на конец дня',
    max_sell_price_eod DOUBLE COMMENT 'Максимальная валидная sell price товара на конец дня после двухэтапной агрегации через SKU-group',
    min_full_price_eod DOUBLE COMMENT 'Минимальная валидная full price товара на конец дня после двухэтапной агрегации через SKU-group',
    max_full_price_eod DOUBLE COMMENT 'Максимальная валидная full price товара на конец дня после двухэтапной агрегации через SKU-group',
    avg_active_sku_sell_price_eod DOUBLE COMMENT 'Среднее sell price доступных SKU с двухэтапной агрегацией через SKU-group',
    min_active_sku_sell_price_eod DOUBLE COMMENT 'Минимальное sell price доступных SKU на конец дня',
    max_active_sku_sell_price_eod DOUBLE COMMENT 'Максимальное sell price доступных SKU на конец дня'
)
USING iceberg
COMMENT 'Silver: дневные price-факты на уровне товара'
PARTITIONED BY (price_date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
