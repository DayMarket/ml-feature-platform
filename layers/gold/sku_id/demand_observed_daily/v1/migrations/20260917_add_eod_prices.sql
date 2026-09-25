-- Цены SKU на конец дня из silver demand_stock_daily (NULL для строк только с продажами).
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS purchase_price_eod BIGINT COMMENT 'Цена покупки для покупателя на конец дня из stock EOD, UZS; NULL без stock-строки или при неизвестной цене';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS sell_price_eod BIGINT COMMENT 'Цена продавца из карточки на конец дня из stock EOD, UZS; NULL без stock-строки или при неизвестной цене';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS full_price_eod BIGINT COMMENT 'Цена до скидки на конец дня из stock EOD, UZS; NULL без stock-строки или при неизвестной цене';
