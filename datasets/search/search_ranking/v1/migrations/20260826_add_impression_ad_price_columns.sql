ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS cpo_adv_version BIGINT COMMENT 'Версия CPO рекламной кампании из event_parameters.cpo_adv_version; NULL, если поле не логировалось событием';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS bid_id BIGINT COMMENT 'Идентификатор рекламной ставки показа из плоской колонки events.bid_id; 0 - ставки не было, NULL - поле не логировалось событием';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS seller_price BIGINT COMMENT 'Цена продавца из event_parameters.seller_price; не равна events.sell_price и берется только из JSON';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS final_price BIGINT COMMENT 'Итоговая цена показа: COALESCE(event_parameters.final_price, seller_price, full_price)';
