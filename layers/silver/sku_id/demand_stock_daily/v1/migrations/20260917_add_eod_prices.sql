-- Цены SKU на конец дня из marts.daily_sku_quantity_eod для строк с положительным остатком.
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS purchase_price_eod BIGINT COMMENT 'Цена покупки для покупателя на конец дня, UZS (daily_sku_quantity_eod.purchase_price_eod); NULL при 0 в источнике';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS sell_price_eod BIGINT COMMENT 'Цена продавца из карточки на конец дня, UZS (daily_sku_quantity_eod.sell_price_eod); NULL при 0 в источнике';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS full_price_eod BIGINT COMMENT 'Цена до скидки (зачёркнутая) на конец дня, UZS (daily_sku_quantity_eod.full_price_eod); NULL при 0 в источнике';
