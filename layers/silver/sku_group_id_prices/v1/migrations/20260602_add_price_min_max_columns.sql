ALTER TABLE {target_table} ADD COLUMN min_sell_price_eod DOUBLE COMMENT 'Минимальная цена продажи на конец дня по SKU внутри sku group';
ALTER TABLE {target_table} ADD COLUMN max_sell_price_eod DOUBLE COMMENT 'Максимальная цена продажи на конец дня по SKU внутри sku group';
ALTER TABLE {target_table} ADD COLUMN min_full_price_eod DOUBLE COMMENT 'Минимальная полная цена на конец дня по SKU внутри sku group';
ALTER TABLE {target_table} ADD COLUMN max_full_price_eod DOUBLE COMMENT 'Максимальная полная цена на конец дня по SKU внутри sku group';
