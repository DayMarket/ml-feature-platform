ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS sell_price BIGINT COMMENT 'Цена показа из плоской колонки events.sell_price; совпадает с event_parameters.sell_price на всех непустых показах. Не равна seller_price: разные поля одного события, расходятся на 8.4% показов';
