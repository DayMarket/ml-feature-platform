CREATE TABLE IF NOT EXISTS {target_table} (
    price_date DATE COMMENT 'Дата исторического EOD-среза цен в UTC',
    product_id BIGINT COMMENT 'Идентификатор товара',
    avg_sell_price_eod DOUBLE COMMENT 'Среднее sell price по SKU-group товара на конец дня',
    min_full_price_eod DOUBLE COMMENT 'Минимальное full price товара на конец дня',
    min_sell_price_eod DOUBLE COMMENT 'Минимальное sell price товара на конец дня',
    avg_active_sku_sell_price_eod DOUBLE COMMENT 'Среднее sell price доступных SKU с двухэтапной агрегацией через SKU-group',
    min_active_sku_sell_price_eod DOUBLE COMMENT 'Минимальное sell price доступных SKU на конец дня',
    max_active_sku_sell_price_eod DOUBLE COMMENT 'Максимальное sell price доступных SKU на конец дня'
)
USING iceberg
COMMENT 'Silver: дневные price-факты на уровне товара'
PARTITIONED BY (price_date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
